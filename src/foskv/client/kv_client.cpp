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
        co_return std::unexpected{make_client_error(ClientError::kConnectFailed)};
    }
    // co_return KVClient{std::move(has_consumer.value())};
}

auto foskv::client::KVClient::shutdown() -> kosio::async::Task<> {
    co_await consumer_.shutdown();
}

auto foskv::client::KVClient::Put(std::string &&key, std::string &&value) -> kosio::async::Task<void> {
    kv::PutRequest request;
    request.set_key(std::move(key));
    request.set_value(std::move(value));
    request.set_prev_kv(true);
    auto req_payload_size = request.ByteSizeLong();
    if (!request.SerializeToArray(buffer_.data(), req_payload_size)) {
        LOG_ERROR("Failed to serialize request");
        co_return;
    }
    auto ret = co_await consumer_.call(rpc::KVService::ServiceName, rpc::KVService::Put, {buffer_.data(), req_payload_size},
    [this](std::string_view resp_payload) -> kosio::async::Task<void> {
        kv::PutResponse response;
        if (!response.ParseFromArray(resp_payload.data(), resp_payload.size())) {
            LOG_ERROR("Failed to parse response");
            co_return;
        }

        auto prev_kv = response.prev_kv();
        auto cluster_id = response.header().cluster_id();
        auto member_id = response.header().member_id();
        auto term = response.header().term();
        LOG_INFO("Receive Put response from cluster {}, member_id {}, term {}", cluster_id, member_id, term);
        auto success = response.header().success();
        if (!success) {
            LOG_ERROR("{}", make_kv_error(response.header().error_code()));
            if (response.header().has_redirect()) {
                auto redirect = response.header().redirect();
                auto host = redirect.host();
                auto port = static_cast<uint16_t>(redirect.port());
                auto has_consumer = co_await rpc::RpcConsumer::connect(host, port);
                if (!has_consumer) {
                    LOG_ERROR("Failed to redirect to {}:{} : {}", host, port, has_consumer.error());
                    co_return;
                }
                co_await shutdown();
                // consumer_ = std::move(has_consumer.value());
                LOG_INFO("Redirect to {}:{}", host, port);
            } else {
                co_return;
            }
        } else {
            LOG_INFO("Success to put {} {}", prev_kv.key(), prev_kv.value());
        }
    });

    if (!ret) {
        LOG_ERROR("Failed to put {} {} : {}", key, value, ret.error());
    }
}

auto foskv::client::KVClient::Get(std::string &&key) -> kosio::async::Task<void> {
    kv::GetRequest request;
    request.set_key(std::move(key));
    auto req_payload_size = request.ByteSizeLong();
    if (!request.SerializeToArray(buffer_.data(), req_payload_size)) {
        LOG_ERROR("Failed to serialize request");
        co_return;
    }
    auto ret = co_await consumer_.call(rpc::KVService::ServiceName, rpc::KVService::Get, {buffer_.data(), req_payload_size},
    [this](std::string_view resp_payload) -> kosio::async::Task<void> {
        kv::GetResponse response;
        if (!response.ParseFromArray(resp_payload.data(), resp_payload.size())) {
            LOG_ERROR("Failed to parse response");
            co_return;
        }

        auto kvs = response.kvs();
        auto cluster_id = response.header().cluster_id();
        auto member_id = response.header().member_id();
        auto term = response.header().term();
        LOG_INFO("Receive Get response from cluster {}, member_id {}, term {}", cluster_id, member_id, term);
        auto success = response.header().success();
        if (!success) {
            LOG_ERROR("{}", make_kv_error(response.header().error_code()));
            if (response.header().has_redirect()) {
                auto redirect = response.header().redirect();
                auto host = redirect.host();
                auto port = static_cast<uint16_t>(redirect.port());
                auto has_consumer = co_await rpc::RpcConsumer::connect(host, port);
                if (!has_consumer) {
                    LOG_ERROR("Failed to redirect to {}:{} : {}", host, port, has_consumer.error());
                    co_return;
                }
                co_await shutdown();
                // consumer_ = std::move(has_consumer.value());
                LOG_INFO("Redirect to {}:{}", host, port);
            } else {
                co_return;
            }
        } else {
            LOG_INFO("Success to get {} : {}", kvs.key(), kvs.value());
        }
    });

    if (!ret) {
        LOG_ERROR("Failed to get {} : {}", key, ret.error());
    }
}

auto foskv::client::KVClient::Delete(std::string &&key) -> kosio::async::Task<void> {
    kv::DeleteRequest request;
    request.set_key(std::move(key));
    auto req_payload_size = request.ByteSizeLong();
    if (!request.SerializeToArray(buffer_.data(), req_payload_size)) {
        LOG_ERROR("Failed to serialize request");
        co_return;
    }
    auto ret = co_await consumer_.call(rpc::KVService::ServiceName, rpc::KVService::Delete, {buffer_.data(), req_payload_size},
    [this](std::string_view resp_payload) -> kosio::async::Task<void> {
        kv::DeleteResponse response;
        if (!response.ParseFromArray(resp_payload.data(), resp_payload.size())) {
            LOG_ERROR("Failed to parse response");
            co_return;
        }

        auto cluster_id = response.header().cluster_id();
        auto member_id = response.header().member_id();
        auto term = response.header().term();
        LOG_INFO("Receive Delete response from cluster {}, member_id {}, term {}", cluster_id, member_id, term);
        auto success = response.header().success();
        if (!success) {
            LOG_ERROR("{}", make_kv_error(response.header().error_code()));
            if (response.header().has_redirect()) {
                auto redirect = response.header().redirect();
                auto host = redirect.host();
                auto port = static_cast<uint16_t>(redirect.port());
                auto has_consumer = co_await rpc::RpcConsumer::connect(host, port);
                if (!has_consumer) {
                    LOG_ERROR("Failed to redirect to {}:{} : {}", host, port, has_consumer.error());
                    co_return;
                }
                co_await shutdown();
                // consumer_ = std::move(has_consumer.value());
                LOG_INFO("Redirect to {}:{}", host, port);
            } else {
                co_return;
            }
        } else {
            LOG_INFO("Success to delete");
        }
    });

    if (!ret) {
        LOG_ERROR("Failed to delete {} : {}", key, ret.error());
    }
}
