#pragma once
#include "foskv/rpc/session_manager.hpp"

namespace foskv::rpc {
class RpcProvider {
public:
    explicit RpcProvider(const kosio::net::SocketAddr& addr)
        : addr_(addr) {}

    // Delete copy
    RpcProvider(const RpcProvider&) = delete;
    auto operator=(const RpcProvider&) -> RpcProvider& = delete;

    // Delete move
    RpcProvider(RpcProvider&&) = delete;
    auto operator=(RpcProvider&&) -> RpcProvider&  = delete;

public:
    [[REMEMBER_CO_AWAIT]]
    auto run() -> kosio::async::Task<Result<void>>;

public:
    void register_invoke(
        ServiceType service_type,
        MethodType method_type,
        detail::Invoke&& invoke);

    /// @return A session at `session_id`, nullptr if not exist
    auto session_at(uint64_t session_id) const -> std::shared_ptr<detail::Session>;

private:
    auto produce_invoke_tasks(kosio::net::OwnedTcpStreamReader reader,
        std::shared_ptr<detail::Session> session) -> kosio::async::Task<>;
    auto consume_invoke_tasks(kosio::net::OwnedTcpStreamWriter writer,
        std::shared_ptr<detail::Session> session) -> kosio::async::Task<>;

private:
    kosio::net::SocketAddr addr_;
    detail::InvokeMap      invokes_;
    detail::SessionManager session_manager_;
};
} // namespace foskv::rpc