#include "foskv/rpc/consumer.hpp"

auto foskv::rpc::RpcConsumer::connect(std::string_view host, uint16_t port)
-> kosio::async::Task<RpcResult<std::unique_ptr<RpcConsumer>>> {
    auto has_addr = kosio::net::SocketAddr::parse(host, port);
    if (!has_addr) [[unlikely]] {
        co_return std::unexpected{make_rpc_error(RpcError::kConnectFailed)};
    }
    auto has_stream = co_await kosio::net::TcpStream::connect(has_addr.value());
    if (!has_stream) [[unlikely]] {
        co_return std::unexpected{make_rpc_error(RpcError::kConnectFailed)};
    }
    co_return std::make_unique<RpcConsumer>(std::move(has_stream.value()));
}

auto foskv::rpc::RpcConsumer::call(std::string &&service_name, std::string &&method_name, std::string &&payload,
    const std::function<void(const std::string &)> &callback) -> kosio::async::Task<> {
    co_await mutex_.lock();
    std::lock_guard lock(mutex_, std::adopt_lock);
    rpc_tasks_.push(RpcTask{std::move(service_name), std::move(method_name), std::move(payload), callback});
    cv_.notify_one();
}

auto foskv::rpc::RpcConsumer::call_internal(const std::string &service_name, const std::string &method_name,
    const std::string &payload) -> kosio::async::Task<RpcResult<std::string>> {
    // Make rpc header
    RpcHeader header;
    header.set_service_name(service_name);
    header.set_method_name(std::string(method_name));
    header.set_payload_length(payload.size());

    // Send [header length -> rpc header -> payload]
    std::string header_str = header.SerializeAsString();
    uint32_t header_len_net = htonl(static_cast<uint32_t>(header_str.size()));

    auto ret = co_await stream_.write_vectored(
        std::span<const char>(reinterpret_cast<char*>(&header_len_net), sizeof(uint32_t)),
        std::span<const char>(header_str.data(), header_str.size()),
        std::span<const char>(payload.data(), payload.size())
    );

    if (!ret) [[unlikely]] {
        co_return std::unexpected{make_rpc_error(RpcError::kSendFailed)};
    }

    // Recv response length
    uint32_t response_len_net;
    auto has_response_len = co_await stream_.read_exact(
        {reinterpret_cast<char*>(&response_len_net), sizeof(uint32_t)});
    if (!has_response_len) [[unlikely]] {
        co_return std::unexpected{make_rpc_error(RpcError::kReceiveFailed)};
    }

    uint32_t response_len = ntohl(response_len_net);

    // Recv response
    std::string response_str;
    response_str.resize(response_len);
    auto has_response = co_await stream_.read_exact(
        {response_str.data(), response_len});
    if (!has_response) [[unlikely]] {
        co_return std::unexpected{make_rpc_error(RpcError::kReceiveFailed)};
    }
    co_return response_str;
}
