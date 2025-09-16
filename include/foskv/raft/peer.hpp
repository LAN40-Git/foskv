#pragma once
#include "foskv/rpc/consumer.hpp"
#include "foskv/raft/raft.pb.h"

namespace foskv::raft {
class Peer {
public:
    explicit Peer(const std::string& host, uint16_t port);
    Peer(Peer&& other) noexcept;
    Peer& operator=(Peer&& other) noexcept;

public:
    // raft rpc
    auto request_vote_rpc(const std::string& payload, rpc::RpcCallback&& callback) -> kosio::async::Task<>;
    auto append_entries_rpc(const std::string& payload, rpc::RpcCallback&& callback) -> kosio::async::Task<>;
    auto install_snapshot_rpc(const std::string& payload) -> kosio::async::Task<>;

private:
    auto connect() -> kosio::async::Task<RpcResult<void>>;

private:
    std::string host_;
    uint16_t    port_;
    std::unique_ptr<rpc::RpcConsumer> consumer_;
};
} // namespace foskv::raft