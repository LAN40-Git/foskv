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
        // Read request length
        uint32_t request_len_net;
        auto has_request_len = co_await stream.read_exact(
            {reinterpret_cast<char*>(&request_len_net), sizeof(uint32_t)});
        if (!has_request_len) [[unlikely]] {
            LOG_ERROR("{}", has_request_len.error());
            break;
        }

        uint32_t request_len = ntohl(request_len_net);

        // Read request
        if (request_str_.size() < request_len) {
            request_str_.resize(request_len);
        }
        auto has_request = co_await stream.read_exact({
            request_str_.data(), request_len});
        if (!has_request) [[unlikely]] {
            LOG_ERROR("{}", has_request.error());
            break;
        }

        // Parse request
        RpcRequest request;
        if (!request.ParseFromArray(request_str_.data(), request_len)) {
            LOG_ERROR("Failed to parse rpc header");
            break;
        }

        auto service_name = request.service_name();
        auto method_name = request.method_name();
        auto payload = request.payload();

        // Invoke
        auto has_response = invoke(service_name, method_name, payload);
        if (!has_response) [[unlikely]] {
            LOG_ERROR("{}", has_response.error());
            continue;
        }

        // Write Response length
        uint32_t response_len_net = ntohl(has_response.value().size());
        auto ret = co_await stream.write_all(
            {reinterpret_cast<char*>(&response_len_net), sizeof(uint32_t)});
        if (!ret) [[unlikely]] {
            LOG_ERROR("{}", ret.error());
            continue;
        }

        // Write Response
        ret = co_await stream.write_all(has_response.value());
        if (!ret) [[unlikely]] {
            LOG_ERROR("{}", ret.error());
            continue;
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
