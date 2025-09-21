#include "foskv/client/kv_client.hpp"

foskv::client::KVClient::KVClient(KVClient &&other) noexcept
    : consumer_(std::move(other.consumer_)) {}

auto foskv::client::KVClient::operator=(KVClient &&other) noexcept -> KVClient& {
    consumer_ = std::move(other.consumer_);
    return *this;
}

auto foskv::client::KVClient::connect(std::string_view host, uint16_t port)
-> kosio::async::Task<Result<KVClient>> {
    auto has_consumer = co_await rpc::RpcConsumer::create(host, port);
    if (!has_consumer) {
        co_return std::unexpected{has_consumer.error()};
    }
    co_return KVClient{std::move(has_consumer.value())};
}
