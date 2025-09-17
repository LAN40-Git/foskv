#pragma once
#include "foskv/rpc/util.hpp"
#include "foskv/rpc/config.hpp"
#include <kosio/sync.hpp>

namespace foskv::rpc {
using RpcCallback = std::function<kosio::async::Task<>(RpcResult<std::string_view> has_response)>;
// One RpcConsumer will approximately cost 8MB
// Remember to co_await shutdown(), otherwise,
// there is a risk of the program crashing
class RpcConsumer {
    using RpcCallbackMap = std::unordered_map<uint64_t, RpcCallback>;
public:
    explicit RpcConsumer(kosio::net::TcpStream&& stream, const kosio::net::SocketAddr& server_addr);
    ~RpcConsumer();

public:
    /// @brief Asynchronously connect to the rpc server
    /// @param server_addr The address of rpc server
    /// @return An unique wrapped RpcConsumer
    static auto connect(const kosio::net::SocketAddr& server_addr)
    -> kosio::async::Task<kosio::Result<std::unique_ptr<RpcConsumer>>>;

public:
    /// @brief Asynchronously send a rpc request and return
    /// @param service_name Rpc service name
    /// @param method_name Rpc method name
    /// @param payload Serialized (protobuf) rpc request
    /// @param callback Triggered when the corresponding reply is received
    /// @return RpcError or void
    /// @note Thread-safe
    [[REMEMBER_CO_AWAIT]]
    auto call(std::string_view service_name,
              std::string_view method_name,
              std::string_view payload,
              RpcCallback&& callback) -> kosio::async::Task<RpcResult<void>>;

    /// @brief Asynchronously shutdown and never use again
    /// @return RpcError or void
    /// @note Not thread-safe, never forget to call this method
    [[REMEMBER_CO_AWAIT]]
    auto shutdown() -> kosio::async::Task<>;

private:
    /// @brief Asynchronously recv rpc response and trigger the callback in `callbacks_`
    /// @return A coroutine task
    /// @note Use kosio::spawn(run())
    auto run() -> kosio::async::Task<>;

    /// @brief Asynchronously reconnect to the rpc server
    /// @return RpcError or void
    /// @note Remember `co_await`, not thread-safe
    [[REMEMBER_CO_AWAIT]]
    auto reconnect() -> kosio::async::Task<RpcResult<void>>;

private:
    kosio::sync::Mutex     mutex_;
    kosio::sync::Latch     latch_{1};
    std::atomic<bool>      is_shutdown_{false}; // Do not change the default values
    std::atomic<bool>      is_running_{false};  // Do not change the default values
    uint64_t               request_id_{0};
    std::vector<char>      buffer_;
    kosio::net::TcpStream  stream_;
    kosio::net::SocketAddr server_addr_{};
    RpcCallbackMap         callbacks_;
};
} // namespace foskv::rpc