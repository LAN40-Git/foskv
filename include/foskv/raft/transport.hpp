#pragma once
#include <kosio/sync.hpp>
#include "foskv/raft/peer.hpp"

namespace foskv::raft {
class RaftNode;
class Transport {
    friend class RaftNode;
    using PeerMap = std::unordered_map<kosio::net::SocketAddr, Peer>;

public:
    explicit Transport(uint64_t cluster_id, uint64_t member_id, const kosio::net::SocketAddr &addr);

public:
    auto run() -> kosio::async::Task<>;
    auto broadcast_request_vote(RequestVoteRequest&& request, rpc::RpcCallback&& callback) -> kosio::async::Task<>;
    auto broadcast_append_entries(AppendEntriesRequest&& request, rpc::RpcCallback&& callback) -> kosio::async::Task<>;
    auto broadcast_install_snapshot(InstallSnapshotRequest&& request, rpc::RpcCallback&& callback) -> kosio::async::Task<>;

private:
    uint64_t           member_id_;
    PeerMap            peers_;
    rpc::RpcProvider   rpc_provider_;
};
} // namespace foskv::raft