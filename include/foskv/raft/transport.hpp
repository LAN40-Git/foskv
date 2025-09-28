#pragma once
#include "foskv/raft/raft_config.hpp"

namespace foskv::raft {
class RaftNode;
} // namespace foskv::raft

namespace foskv::raft::detail {
class Transport {
    friend class foskv::raft::RaftNode;
    friend class StateMachine;
public:
    explicit Transport(RaftConfig&& config, std::unique_ptr<rpc::RpcProvider> provider);

    // Delete copy
    Transport(const Transport&) = delete;
    auto operator=(const Transport&) -> Transport& = delete;

    Transport(Transport&&) = default;
    auto operator=(Transport&&) -> Transport& = default;

public:
    [[REMEMBER_CO_AWAIT]]
    static auto create(RaftConfig&& config) -> kosio::async::Task<Result<Transport>>;

public:
    [[nodiscard]]
    auto cluster_id() const noexcept -> uint64_t { return config_.cluster_id_; }
    [[nodiscard]]
    auto member_id() const noexcept -> uint64_t { return config_.member_id_; }
    [[nodiscard]]
    auto name() const noexcept -> std::string { return config_.name_; }
    [[nodiscard]]
    auto peer_count() const noexcept -> std::size_t { return config_.peers_.size(); }
    [[nodiscard]]
    auto peer_name(uint64_t member_id) -> std::string {
        auto it = config_.peers_.find(member_id);
        if (it == config_.peers_.end()) {
            return "";
        }
        return it->second.name();
    }

public:
    [[REMEMBER_CO_AWAIT]]
    auto run() const -> kosio::async::Task<Result<void>>;
    [[REMEMBER_CO_AWAIT]]
    auto shutdown() const -> kosio::async::Task<>;

public:
    [[REMEMBER_CO_AWAIT]]
    auto single_append_entries_request(uint64_t to_member_id, AppendEntriesRequest request, Peer::RpcCallback callback) -> kosio::async::Task<>;

public:
    auto broadcast_request_vote_request(RequestVoteRequest request, Peer::RpcCallback callback) -> kosio::async::Task<>;
    auto broadcast_append_entries_request(AppendEntriesRequest request, Peer::RpcCallback callback) -> kosio::async::Task<>;
    auto broadcast_install_snapshot_request(InstallSnapshotRequest request, Peer::RpcCallback callback) -> kosio::async::Task<>;

private:
    RaftConfig                        config_;
    std::unique_ptr<rpc::RpcProvider> provider_;
};
} // namespace foskv::raft::detail