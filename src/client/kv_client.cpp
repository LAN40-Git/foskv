#include "foskv/client/kv_client.hpp"

foskv::client::KVClient::KVClient(KVClient &&other) noexcept
    : consumer_(std::move(other.consumer_)) {}

auto foskv::client::KVClient::operator=(KVClient &&other) noexcept -> KVClient& {
    consumer_ = std::move(other.consumer_);
    return *this;
}

auto foskv::client::KVClient::connect(std::string_view host, uint16_t port)
-> kosio::async::Task<ClientResult<KVClient>> {
    auto has_consumer = co_await rpc::RpcConsumer::connect(host, port);
    if (!has_consumer) {
        LOG_ERROR("Failed to connect to {}:{} : {}", host, port, has_consumer.error());
        co_return std::unexpected{make_client_error(ClientError::)}
    }
}
