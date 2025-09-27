#pragma once
#include "foskv/rpc/service.hpp"
#include "foskv/rpc/consumer.hpp"

namespace foskv::client {
class KVClient {
private:
    explicit KVClient(std::unique_ptr<rpc::RpcConsumer> consumer)
        : consumer_(std::move(consumer)) {}

public:
    KVClient(KVClient&& other) noexcept;
    auto operator=(KVClient&& other) noexcept -> KVClient&;

public:
    [[REMEMBER_CO_AWAIT]]
    static auto Connect(std::string_view host, uint16_t port) -> kosio::async::Task<Result<KVClient>>;

public:
    [[REMEMBER_CO_AWAIT]]
    auto Put(std::string key, std::string value) -> kosio::async::Task<>;
    [[REMEMBER_CO_AWAIT]]
    auto Get(std::string key) -> kosio::async::Task<>;
    [[REMEMBER_CO_AWAIT]]
    auto Delete(std::string key) -> kosio::async::Task<>;

public:
    auto redirect_to(std::string_view host, uint16_t port) -> kosio::async::Task<Result<void>>;

private:
    kosio::sync::Mutex                mutex_;
    std::unique_ptr<rpc::RpcConsumer> consumer_;
};
} // namespace foskv::client