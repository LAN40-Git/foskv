#pragma once
#include <kosio/sync.hpp>
#include "foskv/raft/peer.hpp"
#include "foskv/raft/config.hpp"

namespace foskv::raft {
class RaftNode;
} // namespace foskv::raft

namespace foskv::raft::detail {
class Transport {
    friend class foskv::raft::RaftNode;
    using PeerMap = std::unordered_map<kosio::net::SocketAddr, Peer>;

public:
    explicit Transport(const Config& config);

public:
    auto run() -> kosio::async::Task<>;
    auto broadcast_request_vote(RequestVoteRequest&& request, rpc::RpcCallback&& callback) -> kosio::async::Task<>;
    auto broadcast_append_entries(AppendEntriesRequest&& request, rpc::RpcCallback&& callback) -> kosio::async::Task<>;
    auto broadcast_install_snapshot(InstallSnapshotRequest&& request, rpc::RpcCallback&& callback) -> kosio::async::Task<>;

private:
    uint64_t         cluster_id_;
    uint64_t         member_id_;
    PeerMap          peers_;
    rpc::RpcProvider rpc_provider_;
};
} // namespace foskv::raft::detail