#include "foskv/rpc/provider.hpp"

void foskv::rpc::RpcProvider::register_handler(
    const std::string& service_name,
    const std::string& method_name,
    const Handler &handler) {
    invokers_[service_name][method_name] = handler;
}

auto foskv::rpc::RpcProvider::invoke(
    const std::string& service_name,
    const std::string& method_name,
    std::string_view args,
    std::string& response) -> RpcResult<void> {
    auto invoker = invokers_.find(service_name);
    if (invoker == invokers_.end()) {
        return std::unexpected{make_rpc_error(RpcError::kServiceNotFound)};
    }

    auto handler = invoker->second.find(method_name);
    if (handler == invoker->second.end()) {
        return std::unexpected{make_rpc_error(RpcError::kMethodNotFound)};
    }

    handler->second(args, response);
    return {};
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
        while (true) {
            auto has_stream = co_await listener.accept();
            if (!has_stream) [[unlikely]] {
                LOG_ERROR("{}", ret.error());
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
        // Read RpcHeader length
        uint32_t header_len_net;
        auto has_header_len = co_await stream.read_exact(
            {reinterpret_cast<char*>(&header_len_net), sizeof(uint32_t)});
        if (!has_header_len) [[unlikely]] {
            LOG_ERROR("{}", has_header_len.error());
            break;
        }

        uint32_t header_len = ntohl(header_len_net);

        // Read RpcHeader
        std::string header_str;
        header_str.resize(header_len);
        auto has_header = co_await stream.read_exact({
            header_str.data(), header_str.size()});
        if (!has_header) [[unlikely]] {
            LOG_ERROR("{}", has_header.error());
            break;
        }

        // Parse RpcHeader
        RpcHeader header;
        if (!header.ParseFromString(header_str)) {
            LOG_ERROR("{}", header.error());
            break;
        }

        auto service_name = header.service_name();
        auto method_name = header.method_name();
        auto args_length = header.args_length();
        // Read Args
        std::string args_str;
        args_str.resize(args_length);
        auto has_args = co_await stream.read_exact({
            args_str.data(), args_str.size()});
        if (!has_args) [[unlikely]] {
            LOG_ERROR("{}", has_args.error());
            break;
        }

        // Invoke
        std::string response;
        auto has_invoke = invoke(service_name, method_name, args_str, response);
        if (!has_invoke) [[unlikely]] {
            LOG_ERROR("{}", has_invoke.error());
            continue;
        }

        // Write Response
        auto ret = co_await stream.write_all(response);
        if (!ret) [[unlikely]] {
            LOG_ERROR("{}", ret.error());
            continue;
        }
    }
}
