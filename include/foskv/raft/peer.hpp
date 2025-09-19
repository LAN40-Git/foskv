#pragma once
#include "foskv/rpc/rpc.hpp"
#include "foskv/storage/storage.hpp"

namespace foskv::raft::detail {
class Peer {
public:
    explicit Peer(uint64_t member_id, std::string_view name, std::string_view host, uint16_t port, rpc::RpcConsumer&& consumer);
    Peer(Peer&& other) noexcept;
    Peer& operator=(Peer&& other) noexcept;

public:
    auto member_id() const noexcept -> uint64_t { return member_id_; }
    auto name() const noexcept -> std::string { return name_; }
    auto host() const noexcept -> std::string { return host_; }
    auto port() const noexcept -> uint16_t { return port_; }

public:
    static auto create(uint64_t member_id, std::string_view name,
        std::string_view host, uint16_t port) -> kosio::async::Task<RaftResult<Peer>>;

public:
    // raft rpc
    auto request_vote(std::string_view req_payload, rpc::RpcCallback&& callback) -> kosio::async::Task<>;
    auto append_entries(std::string_view req_payload, rpc::RpcCallback&& callback) -> kosio::async::Task<>;
    auto install_snapshot(std::string_view req_payload, rpc::RpcCallback&& callback) -> kosio::async::Task<>;

private:
    uint64_t         member_id_;
    std::string      name_;
    std::string      host_;
    uint16_t         port_;
    rpc::RpcConsumer consumer_;
};
} // namespace foskv::raft::detail