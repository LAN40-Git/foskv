#include "foskv/rpc/provider.hpp"

void foskv::rpc::RpcProvider::register_invoke(
    const std::string& service_name,
    const std::string& method_name,
    const Invoke &invoke) {
    invokes_[service_name][method_name] = invoke;
}

void foskv::rpc::RpcProvider::run() {
    kosio::runtime::MultiThreadBuilder::default_create().block_on([this]() -> kosio::async::Task<> {
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
    }());
}

auto foskv::rpc::RpcProvider::handle_rpc(kosio::net::TcpStream stream)
-> kosio::async::Task<> {
    while (true) {
        // Recv rpc header length
        uint32_t header_len_net;
        auto has_request_len = co_await stream.read_exact(
            {reinterpret_cast<char*>(&header_len_net), sizeof(uint32_t)});
        if (!has_request_len) [[unlikely]] {
            LOG_ERROR("{}", has_request_len.error());
            break;
        }

        uint32_t header_len = ntohl(header_len_net);

        // Recv rpc header
        std::string header_str;
        header_str.resize(header_len);
        auto has_request = co_await stream.read_exact(
            {header_str.data(), header_len});
        if (!has_request) [[unlikely]] {
            LOG_ERROR("{}", has_request.error());
            break;
        }

        // Parse rpc header
        RpcHeader header;
        if (!header.ParseFromArray(header_str.data(), header_str.size())) {
            LOG_ERROR("Failed to parse rpc header");
            break;
        }

        auto service_name = header.service_name();
        auto method_name = header.method_name();
        auto payload_length = header.payload_length();

        // Recv payload
        std::string payload;
        payload.resize(payload_length);
        auto has_payload = co_await stream.read_exact(
            {payload.data(), payload_length});
        if (!has_payload) [[unlikely]] {
            LOG_ERROR("{}", has_payload.error());
            break;
        }

        // Invoke
        auto has_response = invoke(service_name, method_name, payload);
        if (!has_response) [[unlikely]] {
            LOG_ERROR("{}", has_response.error());
            break;
        }

        // Send response
        auto& response_str = has_response.value();
        uint32_t response_len_net = htonl(static_cast<uint32_t>(response_str.size()));

        auto ret = co_await stream.write_vectored(
            std::span<const char>(reinterpret_cast<char*>(&response_len_net), sizeof(uint32_t)),
            std::span<const char>(response_str.data(), response_str.size())
        );

        if (!ret) [[unlikely]] {
            LOG_ERROR("{}", ret.error());
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

    std::string response_str;
    invoke->second(payload, response_str);
    return response_str;
}
