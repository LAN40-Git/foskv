#pragma once
#include "foskv/rpc/call_task.hpp"

namespace foskv::rpc {
// A running RpcConsumer takes up about 4MB of memory,
// remember to co_await shutdown(), otherwise the program may crash
class RpcConsumer {
public:
    explicit RpcConsumer(const kosio::net::SocketAddr& server_addr, kosio::net::TcpStream stream)
        : server_addr_(server_addr), stream_(std::move(stream)) {
        callbacks_.rehash(detail::DEFAULT_CALLBACKS_HASH_SIZE);
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

    /// @brief Shutdown the consumer and never use it again
    /// @note Never forget to call this method
    [[REMEMBER_CO_AWAIT]]
    auto shutdown() -> kosio::async::Task<>;

private:
    void run();
    [[REMEMBER_CO_AWAIT]]
    auto connect() -> kosio::async::Task<Result<void>>;
    auto consume_callbacks() -> kosio::async::Task<>;

private:
    detail::RpcCallbackMap callbacks_;
    uint64_t               request_id_{0};
    kosio::net::SocketAddr server_addr_;
    kosio::sync::Mutex     mutex_;
    std::atomic<bool>      is_shutdown_{false};
    std::atomic<bool>      is_running_{false};
    kosio::net::TcpStream  stream_;
};
} // namespace foskv::rpc