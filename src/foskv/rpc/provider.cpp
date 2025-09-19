#include "foskv/rpc/provider.hpp"
#include <kosio/runtime/runtime.hpp>

foskv::rpc::RpcProvider::RpcProvider(RpcProvider &&other) noexcept
    : addr_(other.addr_)
    , invokes_(std::move(other.invokes_)) {
    other.addr_ = kosio::net::SocketAddr{};
}

auto foskv::rpc::RpcProvider::operator=(RpcProvider &&other) noexcept -> RpcProvider & {
    addr_ = other.addr_;
    invokes_ = std::move(other.invokes_);
    other.addr_ = kosio::net::SocketAddr{};
    return *this;
}

auto foskv::rpc::RpcProvider::run() -> kosio::async::Task<> {
    auto has_listener = kosio::net::TcpListener::bind(addr_);
    if (!has_listener) [[unlikely]] {
        throw std::system_error(errno, std::system_category());
    }
    auto listener = std::move(has_listener.value());
    LOG_INFO("Listening on {}...", addr_);
    while (true) {
        auto has_stream = co_await listener.accept();
        if (!has_stream) [[unlikely]] {
            throw std::runtime_error("Failed to accept connection.");
        }
        auto& [stream, peer_addr] = has_stream.value();
        LOG_INFO("Accept connection from {}", peer_addr);
        kosio::spawn(handle_rpc(std::move(stream)));
    }
}

void foskv::rpc::RpcProvider::register_invoke(
    std::string_view service_name,
    std::string_view method_name,
    Invoke&& invoke) {
    invokes_[service_name][method_name] = std::move(invoke);
}

auto foskv::rpc::RpcProvider::handle_rpc(kosio::net::TcpStream stream)
-> kosio::async::Task<> {
    std::array<char, detail::MAX_RPC_MESSAGE_SIZE> buffer;
    std::array<char, detail::MAX_RPC_MESSAGE_SIZE> resp_payload;
    while (true) {
        // Recv rpc header size
        uint32_t rpc_header_size_net;
        auto recv_ret = co_await stream.read_exact(
            {reinterpret_cast<char*>(&rpc_header_size_net), sizeof(uint32_t)});
        if (!recv_ret) [[unlikely]] {
            LOG_ERROR("{}", recv_ret.error());
            break;
        }

        uint32_t rpc_header_size = ntohl(rpc_header_size_net);
        if (rpc_header_size > buffer.max_size()) [[unlikely]] {
            LOG_ERROR("Message too large.", rpc_header_size);
            break;
        }

        // Recv rpc header
        recv_ret = co_await stream.read_exact(
            {buffer.data(), rpc_header_size});
        if (!recv_ret) [[unlikely]] {
            LOG_ERROR("{}", recv_ret.error());
            break;
        }

        // Parse rpc header
        RpcHeader rpc_header;
        if (!rpc_header.ParseFromArray(buffer.data(), rpc_header_size)) {
            LOG_ERROR("Failed to parse rpc header");
            break;
        }

        auto request_id = rpc_header.request_id();
        auto service_name = rpc_header.service_name();
        auto method_name = rpc_header.method_name();
        auto req_payload_size = rpc_header.payload_size();

        // Recv request payload
        recv_ret = co_await stream.read_exact(
            {buffer.data(), req_payload_size});
        if (!recv_ret) [[unlikely]] {
            LOG_ERROR("{}", recv_ret.error());
            break;
        }
        std::string_view req_payload{buffer.data(), req_payload_size};

        // Invoke
        auto service = invokes_.find(service_name);
        if (service == invokes_.end()) [[unlikely]] {
            LOG_ERROR("Failed to find service for {}", service_name);
        }

        auto invoke = service->second.find(method_name);
        if (invoke == service->second.end()) [[unlikely]] {
            LOG_ERROR("Failed to find invoke for {}", method_name);
        }

        std::span<char> resp_payload_span = {resp_payload.data(), resp_payload.max_size()};
        auto has_resp_payload = co_await invoke->second(
            req_payload, resp_payload_span);
        if (!has_resp_payload) [[unlikely]] {
            LOG_ERROR("{} : ({}-{})", has_resp_payload.error(), service_name, method_name);
            continue;
        }

        // Make rpc header
        rpc_header.Clear();
        rpc_header.set_request_id(request_id);
        rpc_header.set_payload_size(resp_payload_span.size());
        rpc_header_size = rpc_header.ByteSizeLong();
        if (!rpc_header.SerializeToArray(buffer.data(), rpc_header_size)) [[unlikely]] {
            LOG_ERROR("Failed to serialize response.");
            continue;
        }

        // Send [rpc header size -> rpc header -> resp_payload]
        rpc_header_size_net = htonl(rpc_header_size);

        auto send_ret = co_await stream.write_vectored(
            std::span<const char>(reinterpret_cast<char*>(&rpc_header_size_net), sizeof(uint32_t)),
            std::span<const char>(buffer.data(), rpc_header_size),
            std::span<const char>(resp_payload_span)
        );

        if (!send_ret) [[unlikely]] {
            LOG_ERROR("{}", send_ret.error());
            break;
        }
    }
    co_await stream.close();
}
