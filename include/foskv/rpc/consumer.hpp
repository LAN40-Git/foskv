#pragma once
#include "foskv/rpc/util.hpp"

namespace foskv::rpc {
// One RpcConsumer will approximately cost 8MB
// Remember to co_await shutdown(), otherwise,
// there is a risk of the program crashing
class RpcConsumer {
public:
    explicit RpcConsumer(const kosio::net::SocketAddr& server_addr)
        : server_addr_(server_addr) {}
    ~RpcConsumer() { assert(is_shutdown_.load(std::memory_order_acquire)); }

    // Delete copy
    RpcConsumer(const RpcConsumer&) = delete;
    auto operator=(const RpcConsumer&) -> RpcConsumer& = delete;

    // Delete move
    RpcConsumer(RpcConsumer&&) = delete;
    auto operator=(RpcConsumer&&) -> RpcConsumer& = delete;

public:
    /// @brief Call a rpc invoke
    /// @param service_name The invoke service name
    /// @param method_name The invoke method name
    /// @param req_payload The request payload
    /// @param callback The callback where receive response
    /// @note Suitable for buffered calls
    [[REMEMBER_CO_AWAIT]]
    auto call(std::string_view service_name,
              std::string_view method_name,
              std::string_view req_payload,
              detail::RpcCallback&& callback) -> kosio::async::Task<RpcResult<void>>;

    /// @brief Call a rpc invoke
    /// @param service_name The invoke service name
    /// @param method_name The invoke method name
    /// @param req_payload The request payload
    /// @param callback The callback where receive response
    /// @note Suitable for no buffered calls
    [[REMEMBER_CO_AWAIT]]
    auto call(std::string&& service_name,
              std::string&& method_name,
              std::string&& req_payload,
              detail::RpcCallback&& callback) -> kosio::async::Task<RpcResult<void>>;

    /// @brief Shutdown the consumer and never use it again
    /// @note Never forget to call this method
    [[REMEMBER_CO_AWAIT]]
    auto shutdown() -> kosio::async::Task<>;

private:
    [[REMEMBER_CO_AWAIT]]
    auto connect() -> kosio::async::Task<RpcResult<void>>;
    auto produce_callbacks(kosio::net::OwnedTcpStreamWriter writer) -> kosio::async::Task<>;
    auto consume_callbacks(kosio::net::OwnedTcpStreamReader reader) -> kosio::async::Task<>;

private:
    std::atomic<int>                  fd_{-1};
    ConcurrentQueue<detail::CallTask> tasks_;
    detail::RpcCallbackMap            callbacks_;
    uint64_t                          request_id_{0};
    kosio::net::SocketAddr            server_addr_;
    kosio::sync::Latch                latch_{2};
    std::atomic<bool>                 is_shutdown_{false};
    std::atomic<bool>                 is_producing_{false};
    std::atomic<bool>                 is_consuming_{false};
};
} // namespace foskv::rpc