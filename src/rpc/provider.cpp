#include "foskv/rpc/provider.hpp"

auto foskv::rpc::RpcProvider::run() -> kosio::async::Task<kosio::Result<kosio::Error>> {
    auto has_addr = kosio::net::SocketAddr::parse(host_, port_);
    if (!has_addr) [[unlikely]] {
        co_return std::unexpected{has_addr.error()};
    }
    auto has_listener = kosio::net::TcpListener::bind(has_addr.value());
    if (!has_listener) [[unlikely]] {
        co_return std::unexpected{has_listener.error()};
    }
    auto listener = std::move(has_listener.value());
    LOG_INFO("Listening on {}...", listener.local_addr().value());
    while (true) {
        auto has_stream = co_await listener.accept();
        if (!has_stream) [[unlikely]] {
            co_return std::unexpected{has_stream.error()};
        }
        auto& [stream, peer_addr] = has_stream.value();
        LOG_INFO("Accept connection from {}", peer_addr);
        kosio::spawn(handle_rpc(std::move(stream)));
    }
}

void foskv::rpc::RpcProvider::register_invoke(
    int fd,
    std::string_view service_name,
    std::string_view method_name,
    Invoke&& invoke) {
    invokes_[fd][service_name][method_name] = std::move(invoke);
}

auto foskv::rpc::RpcProvider::handle_rpc(kosio::net::TcpStream stream)
-> kosio::async::Task<> {
    std::vector<char> buffer(detail::MAX_RPC_MESSAGE_SIZE);
    std::vector<char> resp_buffer(detail::MAX_RPC_MESSAGE_SIZE);
    while (true) {
        // Recv request header size
        uint32_t req_header_size_net;
        auto recv_ret = co_await stream.read_exact(
            {reinterpret_cast<char*>(&req_header_size_net), sizeof(uint32_t)});
        if (!recv_ret) [[unlikely]] {
            LOG_ERROR("{}", recv_ret.error());
            break;
        }

        uint32_t req_header_size = ntohl(req_header_size_net);
        if (req_header_size > buffer.capacity()) [[unlikely]] {
            LOG_ERROR("Message too large.", req_header_size);
            break;
        }

        // Recv request header
        recv_ret = co_await stream.read_exact(
            {buffer.data(), req_header_size});
        if (!recv_ret) [[unlikely]] {
            LOG_ERROR("{}", recv_ret.error());
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
        recv_ret = co_await stream.read_exact(
            {buffer.data(), req_payload_size});
        if (!recv_ret) [[unlikely]] {
            LOG_ERROR("{}", recv_ret.error());
            break;
        }

        // Invoke
        auto has_resp_payload = invoke(
            stream.fd(),
            service_name, method_name,
            {buffer.data(), req_payload_size},
            {resp_buffer.data(), resp_buffer.capacity()});
        if (!has_resp_payload) [[unlikely]] {
            LOG_ERROR("{} : ({}-{})", has_resp_payload.error(), service_name, method_name);
            continue;
        }
        auto resp_payload_size = has_resp_payload.value();
        if (resp_payload_size > buffer.capacity()) [[unlikely]] {
            LOG_ERROR("Message too large : {}", resp_payload_size);
            continue;
        }

        // Make response header
        ResponseHeader resp_header;
        resp_header.set_request_id(request_id);
        resp_header.set_payload_size(resp_payload_size);
        auto resp_header_size = resp_header.ByteSizeLong();
        if (!resp_header.SerializeToArray(buffer.data(), resp_header_size)) [[unlikely]] {
            LOG_ERROR("Failed to serialize response.");
            continue;
        }

        // Send [resp_header_len_net -> resp_header -> resp_payload]
        uint32_t resp_header_size_net = htonl(static_cast<uint32_t>(resp_header_size));

        auto send_ret = co_await stream.write_vectored(
            std::span<const char>(reinterpret_cast<char*>(&resp_header_size_net), sizeof(uint32_t)),
            std::span<const char>(buffer.data(), resp_header_size),
            std::span<const char>(resp_buffer.data(), resp_payload_size)
        );

        if (!send_ret) [[unlikely]] {
            LOG_ERROR("{}", send_ret.error());
            break;
        }
    }
    // Remove the registered file descriptor, here
    // is thread safe since the connection is not closed yet
    invokes_.erase(stream.fd());
    co_await stream.close();
}

auto foskv::rpc::RpcProvider::invoke(
    int fd,
    std::string_view service_name,
    std::string_view method_name,
    std::string_view payload,
    std::span<char> response) -> RpcResult<std::size_t> {
    auto connection = invokes_.find(fd);
    if (connection == invokes_.end()) [[unlikely]] {
        return std::unexpected{make_rpc_error(RpcError::kFdNotRegister)};
    }

    auto service = connection->second.find(service_name);
    if (service == connection->second.end()) [[unlikely]] {
        return std::unexpected{make_rpc_error(RpcError::kServiceNotFound)};
    }

    auto invoke = service->second.find(method_name);
    if (invoke == service->second.end()) [[unlikely]] {
        return std::unexpected{make_rpc_error(RpcError::kMethodNotFound)};
    }

    return invoke->second(payload, response);
}
