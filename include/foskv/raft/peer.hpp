#pragma once
#include "foskv/raft/util.hpp"

namespace foskv::raft::detail {
class Peer {
public:
    explicit Peer(uint64_t member_id, std::string_view name, const kosio::net::SocketAddr& addr);
    Peer(Peer&& other) noexcept;
    Peer& operator=(Peer&& other) noexcept;

public:
    auto member_id() const noexcept -> uint64_t { return member_id_; }
    auto name() const noexcept -> std::string_view { return name_; }
    auto addr() const noexcept -> const kosio::net::SocketAddr& { return addr_; }

public:
    static auto create(uint64_t member_id, std::string_view name,
        std::string_view host, uint16_t port) -> RaftResult<Peer>;

public:
    // raft rpc
    auto request_vote(std::string_view req_payload, rpc::RpcCallback&& callback) -> kosio::async::Task<>;
    auto append_entries(std::string_view req_payload, rpc::RpcCallback&& callback) -> kosio::async::Task<>;
    auto install_snapshot(std::string_view req_payload, rpc::RpcCallback&& callback) -> kosio::async::Task<>;

private:
    [[REMEMBER_CO_AWAIT]]
    auto connect() -> kosio::async::Task<kosio::Result<void>>;

private:
    uint64_t                          member_id_;
    std::string                       name_;
    kosio::net::SocketAddr            addr_;
    std::unique_ptr<rpc::RpcConsumer> consumer_{nullptr};
};
} // namespace foskv::raft::detail