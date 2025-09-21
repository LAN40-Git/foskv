#pragma once
#include "foskv/rpc/util.hpp"

namespace foskv::rpc {
// A running RpcConsumer takes up about 8MB of memory,
// remember to co_await shutdown(), otherwise the program may crash
class RpcConsumer {
public:
    explicit RpcConsumer(const kosio::net::SocketAddr& server_addr)
        : server_addr_(server_addr) {}

public:
    ~RpcConsumer() { assert(is_shutdown_.load(std::memory_order_acquire)); }

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
    /// @param service_name The invoke service name
    /// @param method_name The invoke method name
    /// @param req_payload The request payload
    /// @param callback The callback where receive response
    /// @note Suitable for buffered calls
    [[REMEMBER_CO_AWAIT]]
    auto call(std::string_view service_name,
              std::string_view method_name,
              std::string_view req_payload,
              detail::RpcCallback&& callback) -> kosio::async::Task<Result<void>>;

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
              detail::RpcCallback&& callback) -> kosio::async::Task<Result<void>>;

    /// @brief Shutdown the consumer and never use it again
    /// @note Never forget to call this method
    [[REMEMBER_CO_AWAIT]]
    auto shutdown() -> kosio::async::Task<>;

private:
    [[REMEMBER_CO_AWAIT]]
    auto connect() -> kosio::async::Task<Result<void>>;
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