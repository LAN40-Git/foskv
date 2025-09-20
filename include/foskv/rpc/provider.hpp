#pragma once
#include "foskv/rpc/util.hpp"

namespace foskv::rpc {
class RpcProvider : util::Noncopyable {
public:
    explicit RpcProvider(const kosio::net::SocketAddr& addr)
        : addr_(addr) {}

public:
    RpcProvider(RpcProvider&& other) noexcept;
    auto operator=(RpcProvider&& other) noexcept -> RpcProvider&;

public:
    /// @brief Asynchronous accept rpc client connection
    /// @return A coro task, asynchronous accept rpc client connection
    /// @note Remember `co_await`. This function throws an exception when
    ///       it goes wrong, and you need to catch and directly execute
    ///       the logic of the program exit
    [[REMEMBER_CO_AWAIT]]
    auto run() -> kosio::async::Task<>;

public:
    /// @brief Register a rpc invoke
    /// @param service_name Service name
    /// @param method_name Method name
    /// @param invoke The invoke function
    /// @note Not thread-safe
    void register_invoke(
        std::string_view service_name,
        std::string_view method_name,
        detail::Invoke&& invoke);

public:
    [[REMEMBER_CO_AWAIT]]
    auto add_invoke_task(const std::string& addr, detail::InvokeTask&& task) -> kosio::async::Task<>;

private:
    auto produce_invoke_tasks(kosio::net::OwnedTcpStreamReader reader, std::string addr) -> kosio::async::Task<>;
    auto consume_invoke_tasks(kosio::net::OwnedTcpStreamWriter writer, std::string addr) -> kosio::async::Task<>;

private:
    kosio::net::SocketAddr addr_;
    // service_name -> method_name -> invoke
    std::unordered_map<std::string_view, detail::Service> invokes_;
    std::unordered_map<std::string, ConcurrentQueue<detail::InvokeTask>> task_queues_;
};
} // namespace foskv::rpc