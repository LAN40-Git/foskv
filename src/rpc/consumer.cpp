#include "foskv/rpc/consumer.hpp"

auto foskv::rpc::RpcConsumer::connect(std::string_view host, uint16_t port)
-> kosio::async::Task<kosio::Result<RpcConsumer, kosio::IoError>> {
    auto has_addr = kosio::net::SocketAddr::parse(host, port);
    if (!has_addr) {
        co_return std::unexpected{has_addr.error()};
    }
    auto has_stream = co_await kosio::net::TcpStream::connect(has_addr.value());
    if (!has_stream) {
        co_return std::unexpected{has_stream.error()};
    }
    co_return RpcConsumer{std::move(has_stream.value())};
}
