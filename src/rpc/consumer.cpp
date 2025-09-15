#include "foskv/rpc/consumer.hpp"

auto foskv::rpc::RpcConsumer::connect(std::string_view host, uint16_t port)
-> kosio::async::Task<RpcResult<RpcConsumer>> {
    auto has_addr = kosio::net::SocketAddr::parse(host, port);
    if (!has_addr) [[unlikely]] {
        co_return std::unexpected{make_rpc_error(RpcError::kConnectFailed)};
    }
    auto has_stream = co_await kosio::net::TcpStream::connect(has_addr.value());
    if (!has_stream) [[unlikely]] {
        co_return std::unexpected{make_rpc_error(RpcError::kConnectFailed)};
    }
    co_return RpcConsumer{std::move(has_stream.value())};
}
