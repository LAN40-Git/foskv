#include "foskv/client/kv_client.hpp"

foskv::client::KVClient::KVClient(KVClient &&other) noexcept
    : consumer_(std::move(other.consumer_)) {}

auto foskv::client::KVClient::operator=(KVClient &&other) noexcept -> KVClient& {
    consumer_ = std::move(other.consumer_);
    return *this;
}

auto foskv::client::KVClient::Connect(std::string_view host, uint16_t port)
-> kosio::async::Task<Result<KVClient>> {
    auto has_consumer = co_await rpc::RpcConsumer::create(host, port);
    if (!has_consumer) {
        co_return std::unexpected{has_consumer.error()};
    }
    co_return KVClient{std::move(has_consumer.value())};
}

auto foskv::client::KVClient::Put(std::string key, std::string value) -> kosio::async::Task<> {
    kv::PutRequest request;
    request.set_key(key);
    request.set_value(value);
    using rpc::ServiceType;
    using rpc::MethodType;
    co_await consumer_->call(ServiceType::kKv, MethodType::kKvPut, request.SerializeAsString(),
    [this, key, value](std::string_view resp_payload) -> kosio::async::Task<> {
        kv::PutResponse response;
        if (!response.ParseFromArray(resp_payload.data(), resp_payload.size())) {
            LOG_ERROR("Failed to parse put response");
            co_return;
        }

        if (response.header().success()) {
            LOG_INFO("Put successful, key : {}, value : {}.", key, value);
        } else {
            // Handle redirect
            if (response.header().has_redirect()) {
                auto redirect = response.header().redirect();
                auto host = redirect.host();
                auto port = redirect.port();
                if (auto ret = co_await redirect_to(host, port); !ret) {
                    LOG_ERROR("Failed to redirect : {}", ret.error());
                }
                co_await this->Put(key, value);
            } else {
                LOG_ERROR("{}", rpc::RpcError{static_cast<int>(response.header().error_code())}.message());
            }
        }
    });
}

auto foskv::client::KVClient::Get(std::string key) -> kosio::async::Task<> {
    kv::GetRequest request;
    request.set_key(key);
    using rpc::ServiceType;
    using rpc::MethodType;
    co_await consumer_->call(ServiceType::kKv, MethodType::kKvGet, request.SerializeAsString(),
    [this, key](std::string_view resp_payload) -> kosio::async::Task<> {
        kv::GetResponse response;
        if (!response.ParseFromArray(resp_payload.data(), resp_payload.size())) {
            LOG_ERROR("Failed to parse get response");
            co_return;
        }

        if (response.header().success()) {
            LOG_INFO("Get succesful, key : {} value : {}", response.kv().key(), response.kv().value());
        } else {
            // Handle redirect
            if (response.header().has_redirect()) {
                auto redirect = response.header().redirect();
                auto host = redirect.host();
                auto port = redirect.port();
                LOG_INFO("Redirecting to {}:{}", host, port);
                if (auto ret = co_await redirect_to(host, port); !ret) {
                    LOG_ERROR("Failed to redirect : {}", ret.error());
                }
                co_await this->Get(key);
            } else {
                LOG_ERROR("{}", rpc::RpcError{static_cast<int>(response.header().error_code())}.message());
            }
            LOG_VERBOSE("{}", rpc::RpcError{static_cast<int>(response.header().error_code())}.message());
        }
    });
}

auto foskv::client::KVClient::Delete(std::string key) -> kosio::async::Task<> {
    kv::DeleteRequest request;
    request.set_key(key);
    using rpc::ServiceType;
    using rpc::MethodType;
    co_await consumer_->call(ServiceType::kKv, MethodType::kKvDelete, request.SerializeAsString(),
    [this, key](std::string_view resp_payload) -> kosio::async::Task<> {
        foskv::kv::DeleteResponse response;
        if (!response.ParseFromArray(resp_payload.data(), resp_payload.size())) {
            LOG_ERROR("Failed to parse delete response");
            co_return;
        }

        if (response.header().success()) {
            LOG_INFO("Delete succesful, key : {}", key);
        } else {
            // Handle redirect
            if (response.header().has_redirect()) {
                auto redirect = response.header().redirect();
                auto host = redirect.host();
                auto port = redirect.port();
                LOG_INFO("Redirecting to {}:{}", host, port);
                if (auto ret = co_await redirect_to(host, port); !ret) {
                    LOG_ERROR("Failed to redirect : {}", ret.error());
                }
                co_await this->Delete(key);
            } else {
                LOG_ERROR("{}", rpc::RpcError{static_cast<int>(response.header().error_code())}.message());
            }
            LOG_ERROR("{}", rpc::RpcError{static_cast<int>(response.header().error_code())}.message());
        }
    });
}

auto foskv::client::KVClient::redirect_to(std::string_view host, uint16_t port) -> kosio::async::Task<Result<void>> {
    co_await mutex_.lock();
    std::lock_guard lock(mutex_, std::adopt_lock);
    co_await consumer_->shutdown();
    auto has_consumer = co_await rpc::RpcConsumer::create(host, port);
    if (!has_consumer) {
        co_return std::unexpected{has_consumer.error()};
    }
    // Take tasks
    auto tasks = co_await consumer_->take_tasks();
    consumer_ = std::move(has_consumer.value());
    // Put tasks
    co_await consumer_->put_tasks(std::move(tasks));
    co_return Result<void>{};
}
