#pragma once
#include "foskv/rpc/rpc.hpp"
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
    static auto connect(std::string_view host, uint16_t port) -> kosio::async::Task<Result<KVClient>>;

private:
    std::unique_ptr<rpc::RpcConsumer> consumer_;
};
} // namespace foskv::client