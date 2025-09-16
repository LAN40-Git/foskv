#include "foskv/rpc/provider.hpp"

auto foskv::rpc::RpcProvider::event_loop() -> kosio::async::Task<> {
    kosio::spawn(run());
    co_await kosio::signal::ctrl_c();
}

void foskv::rpc::RpcProvider::register_invoke(
    const std::string& service_name,
    const std::string& method_name,
    const Invoke &invoke) {
    invokes_[service_name][method_name] = invoke;
}

auto foskv::rpc::RpcProvider::run() -> kosio::async::Task<> {
    auto has_addr = kosio::net::SocketAddr::parse(host_, port_);
    if (!has_addr) {
        LOG_ERROR("{}", has_addr.error());
        co_return;
    }
    auto has_listener = kosio::net::TcpListener::bind(has_addr.value());
    if (!has_listener) {
        LOG_ERROR("{}", has_listener.error());
        co_return;
    }
    auto listener = std::move(has_listener.value());
    LOG_INFO("Listening on {}...", listener.local_addr().value());
    while (true) {
        auto has_stream = co_await listener.accept();
        if (!has_stream) [[unlikely]] {
            LOG_ERROR("{}", has_stream.error());
            break;
        }
        auto& [stream, peer_addr] = has_stream.value();
        LOG_INFO("Accept connection from {}", peer_addr);
        kosio::spawn(handle_rpc(std::move(stream)));
    }
}

auto foskv::rpc::RpcProvider::handle_rpc(kosio::net::TcpStream stream)
-> kosio::async::Task<> {
    std::vector<char> buffer(4 * 1024 * 1024);
    auto peer_addr = stream.peer_addr().value_or(kosio::net::SocketAddr{});
    while (true) {
        // Recv request header size
        uint32_t req_header_size_net;
        auto recv_ret = co_await stream.read(
            {reinterpret_cast<char*>(&req_header_size_net), sizeof(uint32_t)});
        if (!recv_ret || recv_ret.value() == 0) [[unlikely]] {
            if (!recv_ret) {
                LOG_ERROR("{}", recv_ret.error());
            } else {
                LOG_INFO("Connection closed {}", peer_addr);
            }
            break;
        }

        uint32_t req_header_size = ntohl(req_header_size_net);
        if (req_header_size > buffer.capacity()) [[unlikely]] {
            LOG_ERROR("Message too large.", req_header_size);
            break;
        }

        // Recv request header
        recv_ret = co_await stream.read(
            {buffer.data(), req_header_size});
        if (!recv_ret || recv_ret.value() == 0) [[unlikely]] {
            if (!recv_ret) {
                LOG_ERROR("{}", recv_ret.error());
            } else {
                LOG_INFO("Connection closed {}", peer_addr);
            }
            break;
        }

        // Parse request header
        RequestHeader req_header;
        if (!req_header.ParseFromArray(buffer.data(), req_header_size)) {
            LOG_ERROR("Failed to parse rpc header");
            break;
        }

        auto request_id = req_header.request_id();
        auto service_name = req_header.service_name();
        auto method_name = req_header.method_name();
        auto req_payload_size = req_header.payload_size();

        // Recv request payload
        recv_ret = co_await stream.read(
            {buffer.data(), req_payload_size});
        if (!recv_ret || recv_ret.value()) [[unlikely]] {
            if (!recv_ret) {
                LOG_ERROR("{}", recv_ret.error());
            } else {
                LOG_INFO("Connection closed {}", peer_addr);
            }
            break;
        }

        // Invoke
        auto has_resp_payload = invoke(service_name, method_name, {buffer.data(), req_payload_size});
        if (!has_resp_payload) [[unlikely]] {
            LOG_ERROR("{}", has_resp_payload.error());
            break;
        }
        auto& resp_payload = has_resp_payload.value();
        auto resp_payload_size = resp_payload.size();

        // Make response header
        ResponseHeader resp_header;
        resp_header.set_request_id(request_id);
        resp_header.set_payload_size(resp_payload_size);
        auto resp_header_size = resp_header.ByteSizeLong();
        if (resp_header_size > buffer.capacity()) [[unlikely]] {
            LOG_ERROR("Message too large.");
            break;
        }
        if (!resp_header.SerializeToArray(buffer.data(), resp_header_size)) {
            LOG_ERROR("Failed to serialize response.");
            break;
        }

        // Send [resp_header_len_net -> resp_header -> resp_payload]
        uint32_t resp_header_size_net = htonl(static_cast<uint32_t>(resp_header_size));

        auto send_ret = co_await stream.write_vectored(
            std::span<const char>(reinterpret_cast<char*>(&resp_header_size_net), sizeof(uint32_t)),
            std::span<const char>(buffer.data(), resp_header_size),
            std::span<const char>(resp_payload.data(), resp_payload_size)
        );

        if (!send_ret) [[unlikely]] {
            LOG_ERROR("{}", send_ret.error());
            break;
        }
    }
}

auto foskv::rpc::RpcProvider::invoke(
    const std::string& service_name,
    const std::string& method_name,
    std::string_view payload) -> RpcResult<std::string> {
    auto service = invokes_.find(service_name);
    if (service == invokes_.end()) {
        return std::unexpected{make_rpc_error(RpcError::kServiceNotFound)};
    }

    auto invoke = service->second.find(method_name);
    if (invoke == service->second.end()) {
        return std::unexpected{make_rpc_error(RpcError::kMethodNotFound)};
    }

    // TODO: Use thread_loacl std::vector<char>& as parma
    std::string response_str;
    invoke->second(payload, response_str);
    return response_str;
}
