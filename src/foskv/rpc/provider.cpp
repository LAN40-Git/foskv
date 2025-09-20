#include "foskv/rpc/provider.hpp"

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
        // thread safe here
        auto addr = peer_addr.to_string();
        task_queues_.erase(addr);
        auto [owned_reader, owned_writer] = stream.into_split();
        kosio::spawn(produce_invoke_tasks(std::move(owned_reader), addr));
        kosio::spawn(consume_invoke_tasks(std::move(owned_writer), addr));
    }
}

void foskv::rpc::RpcProvider::register_invoke(
    std::string_view service_name,
    std::string_view method_name,
    detail::Invoke&& invoke) {
    invokes_[service_name][method_name] = std::move(invoke);
}

auto foskv::rpc::RpcProvider::add_invoke_task(
    const std::string& addr,
    detail::InvokeTask &&task) -> kosio::async::Task<> {
    auto& tasks = task_queues_[addr];
    co_await tasks.push(std::move(task));
}

auto foskv::rpc::RpcProvider::produce_invoke_tasks(kosio::net::OwnedTcpStreamReader reader, std::string addr)
-> kosio::async::Task<> {
    auto& tasks = task_queues_[addr];
    while (true) {
        // Recv rpc header size
        uint32_t rpc_header_size_net;
        auto recv_ret = co_await reader.read_exact(
            {reinterpret_cast<char*>(&rpc_header_size_net), sizeof(uint32_t)});
        if (!recv_ret) [[unlikely]] {
            LOG_ERROR("{}", recv_ret.error());
            break;
        }

        uint32_t rpc_header_size = ntohl(rpc_header_size_net);
        if (rpc_header_size > detail::MAX_RPC_MESSAGE_SIZE) [[unlikely]] {
            LOG_ERROR("Message too large.", rpc_header_size);
            break;
        }

        detail::InvokeTask task;
        task.req_payload_.resize(rpc_header_size);

        // Recv rpc header
        recv_ret = co_await reader.read_exact(task.req_payload_);
        if (!recv_ret) [[unlikely]] {
            LOG_ERROR("{}", recv_ret.error());
            break;
        }

        // Parse rpc header
        RpcHeader rpc_header;
        if (!rpc_header.ParseFromString(task.req_payload_)) {
            LOG_ERROR("Failed to parse rpc header");
            break;
        }

        auto request_id = rpc_header.request_id();
        auto service_name = rpc_header.service_name();
        auto method_name = rpc_header.method_name();
        auto req_payload_size = rpc_header.payload_size();

        task.request_id_ = request_id;
        task.req_payload_.resize(req_payload_size);

        // Recv request payload
        recv_ret = co_await reader.read_exact(task.req_payload_);
        if (!recv_ret) [[unlikely]] {
            LOG_ERROR("{}", recv_ret.error());
            break;
        }

        // Get invoke
        auto service = invokes_.find(service_name);
        if (service == invokes_.end()) [[unlikely]] {
            LOG_ERROR("Failed to find service for {}", service_name);
        }

        auto invoke = service->second.find(method_name);
        if (invoke == service->second.end()) [[unlikely]] {
            LOG_ERROR("Failed to find invoke for {}", method_name);
        }

        task.invoke_ = invoke->second;
        co_await tasks.push(std::move(task));
    }
}

auto foskv::rpc::RpcProvider::consume_invoke_tasks(kosio::net::OwnedTcpStreamWriter writer, std::string addr) -> kosio::async::Task<> {
    auto& tasks = task_queues_[addr];
    std::array<char, sizeof(RpcHeader)> buffer{};
    std::array<char, detail::MAX_RPC_MESSAGE_SIZE> resp_buffer{};
    while (true) {
        auto task = co_await tasks.pop();

        if (!task.has_resp_payload_) {
            auto has_resp_payload = co_await task.invoke_(
            addr, task.request_id_,task.req_payload_, {resp_buffer.data(), resp_buffer.max_size()});
            if (!has_resp_payload) [[unlikely]] {
                LOG_ERROR("Failed to get resp payload from invoke : {}", has_resp_payload.error());
                continue;
            }

            auto resp_payload_size = has_resp_payload.value();
            // It may be an empty reply because the client request needs to
            // be wrapped in a log and synchronized
            if (resp_payload_size == 0) {
                continue;
            }

            // Make rpc header
            RpcHeader rpc_header;
            rpc_header.set_request_id(task.request_id_);
            rpc_header.set_payload_size(resp_payload_size);
            auto rpc_header_size = rpc_header.ByteSizeLong();
            if (!rpc_header.SerializeToArray(buffer.data(), rpc_header_size)) [[unlikely]] {
                LOG_ERROR("Failed to serialize response.");
                continue;
            }

            // Send [rpc header size -> rpc header -> resp_payload]
            auto rpc_header_size_net = htonl(rpc_header_size);

            auto send_ret = co_await writer.write_vectored(
                std::span<const char>(reinterpret_cast<char*>(&rpc_header_size_net), sizeof(uint32_t)),
                std::span<const char>(buffer.data(), rpc_header_size),
                std::span<const char>(resp_buffer.data(), resp_payload_size)
            );

            if (!send_ret) [[unlikely]] {
                LOG_ERROR("{}", send_ret.error());
                break;
            }
        } else {
            // Make rpc header
            RpcHeader rpc_header;
            rpc_header.set_request_id(task.request_id_);
            rpc_header.set_payload_size(task.resp_payload_.size());
            auto rpc_header_size = rpc_header.ByteSizeLong();
            if (!rpc_header.SerializeToArray(buffer.data(), rpc_header_size)) [[unlikely]] {
                LOG_ERROR("Failed to serialize response.");
                continue;
            }

            // Send [rpc header size -> rpc header -> resp_payload]
            auto rpc_header_size_net = htonl(rpc_header_size);

            auto send_ret = co_await writer.write_vectored(
                std::span<const char>(reinterpret_cast<char*>(&rpc_header_size_net), sizeof(uint32_t)),
                std::span<const char>(buffer.data(), rpc_header_size),
                std::span<const char>(task.resp_payload_.data(), task.resp_payload_.size())
            );

            if (!send_ret) [[unlikely]] {
                LOG_ERROR("{}", send_ret.error());
                break;
            }
        }
    }
}
