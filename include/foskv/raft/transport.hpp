#pragma once
#include "foskv/raft/config.hpp"

namespace foskv::raft {
class RaftNode;
} // namespace foskv::raft

namespace foskv::raft::detail {
class Transport {
    friend class foskv::raft::RaftNode;
    friend class StateMachine;
public:
    explicit Transport(Config&& config);

    // Delete copy
    Transport(const Transport&) = delete;
    auto operator=(const Transport&) -> Transport& = delete;

    // Delete move
    Transport(Transport &&other) = delete;
    auto operator=(Transport &&other) -> Transport & = delete;

public:
    auto cluster_id() const noexcept -> uint64_t { return config_.cluster_id_; }
    auto member_id() const noexcept -> uint64_t { return config_.local_member_id_; }
    auto name() const noexcept -> std::string { return config_.local_name_; }
    auto peer_count() const noexcept -> std::size_t { return config_.peers_.size(); }

public:
    auto run() -> kosio::async::Task<kosio::Result<void>>;
    auto broadcast_request_vote_request(RequestVoteRequest&& request, Peer::RpcCallback&& callback) -> kosio::async::Task<>;
    auto broadcast_append_entries_request(AppendEntriesRequest&& request, Peer::RpcCallback&& callback) -> kosio::async::Task<>;
    auto broadcast_install_snapshot_request(InstallSnapshotRequest&& request, Peer::RpcCallback&& callback) -> kosio::async::Task<>;

private:
    Config           config_;
    rpc::RpcProvider provider_;
};
} // namespace foskv::raft::detail