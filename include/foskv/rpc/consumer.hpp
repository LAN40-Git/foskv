#pragma once
#include "foskv/rpc/call_task.hpp"

namespace foskv::rpc {
// A running RpcConsumer takes up about 8MB of memory,
// remember to co_await shutdown(), otherwise the program may crash
class RpcConsumer {
public:
    explicit RpcConsumer(const kosio::net::SocketAddr& server_addr)
        : server_addr_(server_addr) {
        callbacks_.rehash(detail::DEFAULT_CALLBACKS_HASH_SIZE);
    }

public:
    ~RpcConsumer() {
        // assert(is_shutdown_.load(std::memory_order_acquire));
    }

    // Delete copy
    RpcConsumer(const RpcConsumer&) = delete;
    auto operator=(const RpcConsumer&) -> RpcConsumer& = delete;

    // Delete move
    RpcConsumer(RpcConsumer&&) = delete;
    auto operator=(RpcConsumer&&) -> RpcConsumer& = delete;

public:
    [[REMEMBER_CO_AWAIT]]
    static auto create(std::string_view host, uint16_t port) -> kosio::async::Task<Result<std::unique_ptr<RpcConsumer>>>;

public:
    /// @brief Call a rpc invoke
    /// @param service_type The invoke service type
    /// @param method_type The invoke method type
    /// @param req_payload The request payload
    /// @param callback The callback where receive response
    /// @note This version does not move the `req_payload`
    /// and`callback`, suitable for muilt-shot call
    [[REMEMBER_CO_AWAIT]]
    auto call(ServiceType service_type,
              MethodType method_type,
              std::string_view req_payload,
              const RpcCallback& callback) -> kosio::async::Task<Result<void>>;

    /// @brief Call a rpc invoke
    /// @param service_type The invoke service type
    /// @param method_type The invoke method type
    /// @param req_payload The request payload
    /// @param callback The callback where receive response
    /// @note This version does not move the `req_payload`,
    /// suitable for buffered one-shot call
    [[REMEMBER_CO_AWAIT]]
    auto call(ServiceType service_type,
              MethodType method_type,
              std::string_view req_payload,
              RpcCallback&& callback) -> kosio::async::Task<Result<void>>;

    /// @brief Call a rpc invoke
    /// @param service_type The invoke service type
    /// @param method_type The invoke method type
    /// @param req_payload The request payload
    /// @param callback The callback where receive response
    /// @note This version move both `req_payload` and `callback`,
    /// suitable for no-buffered one-shot call
    [[REMEMBER_CO_AWAIT]]
    auto call(ServiceType service_type,
              MethodType method_type,
              std::string&& req_payload,
              RpcCallback&& callback) -> kosio::async::Task<Result<void>>;

    /// @brief Shutdown the consumer and never use it again
    /// @note Never forget to call this method
    [[REMEMBER_CO_AWAIT]]
    auto shutdown() -> kosio::async::Task<>;

    [[REMEMBER_CO_AWAIT]]
    auto redirect(std::string_view host, uint16_t port) -> Result<void>;

private:
    [[REMEMBER_CO_AWAIT]]
    auto connect() -> kosio::async::Task<Result<void>>;
    auto produce_callbacks(kosio::net::OwnedTcpStreamWriter writer) -> kosio::async::Task<>;
    auto consume_callbacks(kosio::net::OwnedTcpStreamReader reader) -> kosio::async::Task<>;

private:
    util::SPSCQueue<detail::CallTask> tasks_;
    detail::RpcCallbackMap            callbacks_;
    uint64_t                          request_id_{0};
    kosio::net::SocketAddr            server_addr_;
    std::atomic<int>                  fd_{-1};
    kosio::sync::Mutex                mutex_;
    kosio::sync::Latch                latch_{2};
    std::atomic<bool>                 is_shutdown_{false};
    std::atomic<bool>                 is_producing_{false};
    std::atomic<bool>                 is_consuming_{false};
};
} // namespace foskv::rpc