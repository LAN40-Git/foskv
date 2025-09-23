#pragma once
#include "foskv/raft/util.hpp"

namespace foskv::raft::detail {
class Peer {
    using RpcCallback = rpc::RpcCallback;
    friend class Transport;
private:
    explicit Peer(uint64_t member_id, std::string_view name,
        std::string_view host, uint16_t port,
        std::unique_ptr<rpc::RpcConsumer> consumer);

public:
    Peer(const Peer&) = delete;
    auto operator=(const Peer&) -> Peer& = delete;
    Peer(Peer&&) = default;
    Peer& operator=(Peer&&) = default;

public:
    [[REMEMBER_CO_AWAIT]]
    static auto create(uint64_t member_id, std::string_view name,
        std::string_view host, uint16_t port) -> kosio::async::Task<Result<Peer>>;

public:
    [[REMEMBER_CO_AWAIT]]
    auto shutdown() const -> kosio::async::Task<>;

public:
    [[nodiscard]]
    auto member_id() const noexcept -> uint64_t { return member_id_; }
    [[nodiscard]]
    auto name() const noexcept -> std::string { return name_; }
    [[nodiscard]]
    auto host() const noexcept -> std::string { return host_; }
    [[nodiscard]]
    auto port() const noexcept -> uint16_t { return port_; }

public:
    // raft rpc
    [[REMEMBER_CO_AWAIT]]
    auto request_vote(std::string_view req_payload, RpcCallback&& callback) const -> kosio::async::Task<>;
    [[REMEMBER_CO_AWAIT]]
    auto append_entries(std::string_view req_payload, RpcCallback&& callback) const -> kosio::async::Task<>;
    [[REMEMBER_CO_AWAIT]]
    auto install_snapshot(std::string_view req_payload, RpcCallback&& callback) const -> kosio::async::Task<>;
    [[REMEMBER_CO_AWAIT]]
    auto request_vote(std::string&& req_payload, RpcCallback&& callback) const -> kosio::async::Task<>;
    [[REMEMBER_CO_AWAIT]]
    auto append_entries(std::string&& req_payload, RpcCallback&& callback) const -> kosio::async::Task<>;
    [[REMEMBER_CO_AWAIT]]
    auto install_snapshot(std::string&& req_payload, RpcCallback&& callback) const -> kosio::async::Task<>;

private:
    uint64_t                          member_id_;
    std::string                       name_;
    std::string                       host_;
    uint16_t                          port_;
    std::unique_ptr<rpc::RpcConsumer> consumer_;
};
} // namespace foskv::raft::detail