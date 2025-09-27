#pragma once
#include "foskv/rpc/session_manager.hpp"

namespace foskv::rpc {
class RpcProvider {
public:
    explicit RpcProvider(const kosio::net::SocketAddr& addr, kosio::net::TcpListener listener)
        : addr_(addr), listener_(std::move(listener)) {}

    // Delete copy
    RpcProvider(const RpcProvider&) = delete;
    auto operator=(const RpcProvider&) -> RpcProvider& = delete;

    // Delete move
    RpcProvider(RpcProvider&&) = delete;
    auto operator=(RpcProvider&&) -> RpcProvider&  = delete;

public:
    [[REMEMBER_CO_AWAIT]]
    static auto create(const kosio::net::SocketAddr& addr) -> kosio::async::Task<Result<std::unique_ptr<RpcProvider>>>;

public:
    [[REMEMBER_CO_AWAIT]]
    auto run() -> kosio::async::Task<Result<void>>;
    [[REMEMBER_CO_AWAIT]]
    auto shutdown() -> kosio::async::Task<void>;

public:
    void register_invoke(
        ServiceType service_type,
        MethodType method_type,
        const detail::Invoke& invoke);

    /// @return A session at `session_id`, nullptr if not exist
    auto session_at(uint64_t session_id) const -> std::shared_ptr<detail::Session>;

private:
    auto produce_invoke_tasks(std::shared_ptr<detail::Session> session) -> kosio::async::Task<>;
    auto consume_invoke_tasks(std::shared_ptr<detail::Session> session) -> kosio::async::Task<>;

private:
    kosio::net::SocketAddr  addr_;
    kosio::net::TcpListener listener_;
    kosio::sync::Mutex      mutex_;
    std::atomic<bool>       is_shutdown_{false};
    std::atomic<bool>       is_running_{false};
    detail::InvokeMap       invokes_;
    detail::SessionManager  session_manager_;
};
} // namespace foskv::rpc