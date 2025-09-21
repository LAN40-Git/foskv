#pragma once
#include "foskv/rpc/util.hpp"

namespace foskv::rpc {
class RpcProvider {
public:
    explicit RpcProvider(const kosio::net::SocketAddr& addr)
        : addr_(addr) {}

    // Delete copy
    RpcProvider(const RpcProvider&) = delete;
    RpcProvider& operator=(const RpcProvider&) = delete;

    // Delete move
    RpcProvider(RpcProvider&&) = delete;
    RpcProvider& operator=(RpcProvider&&) = delete;

public:
    [[REMEMBER_CO_AWAIT]]
    auto run() -> kosio::async::Task<kosio::Result<void>>;

public:
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
    std::unordered_map<std::string_view, detail::Service> invokes_; // See foskv/rpc/rpc.hpp
    std::unordered_map<std::string, ConcurrentQueue<detail::InvokeTask>> task_queues_;
};
} // namespace foskv::rpc