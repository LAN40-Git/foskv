#pragma once
#include "raft_log.hpp"
#include "foskv/raft/transport.hpp"
#include "foskv/raft/persister.hpp"
#include "foskv/raft/state_machine.hpp"

namespace foskv::raft {
class RaftNode {
    friend class detail::StateMachine;
public:
    explicit RaftNode(RaftConfig&& config, detail::StateMachine&& state_machine,
        PersistState&& state, detail::RaftLog&& logs);

public:
    static auto create(std::string_view config_path, std::string_view data_dir) -> kosio::async::Task<Result<std::unique_ptr<RaftNode>>>;

public:
    auto run() -> kosio::async::Task<>;

private:
    auto start_election_timeout() -> kosio::async::Task<>;
    auto start_heartbeat_timeout() -> kosio::async::Task<>;

private:
    void increase_term_to(uint64_t term);
    void become_leader();

private:
    // raft rpc invoke
    [[REMEMBER_CO_AWAIT]]
    auto handle_request_vote_request(std::string_view req_payload, std::span<char> resp_payload)
    -> kosio::async::Task<Result<std::size_t>>;
    [[REMEMBER_CO_AWAIT]]
    auto handle_append_entries_request(std::string_view req_payload, std::span<char> resp_payload)
    -> kosio::async::Task<Result<std::size_t>>;
    [[REMEMBER_CO_AWAIT]]
    auto handle_install_snapshot_request(std::string_view req_payload, std::span<char> resp_payload)
    -> kosio::async::Task<Result<std::size_t>>;

private:
    /* Confirm that you have hold mutex_ */
    auto produce_response_header() const noexcept -> ResponseHeader;
    auto produce_request_vote_request() const noexcept -> RequestVoteRequest;
    auto produce_request_vote_response(bool vote_granted, std::span<char> resp_payload) const noexcept -> Result<std::size_t>;
    auto produce_append_entries_request() const noexcept -> AppendEntriesRequest;
    auto produce_append_entries_response(bool success, std::span<char> resp_payload) const noexcept -> Result<std::size_t>;
    auto produce_install_snapshot_request(uint64_t last_include_index, uint64_t last_include_term,
        uint64_t offset, std::string&& data, bool done) const noexcept -> InstallSnapshotRequest;
    auto produce_install_snapshot_response(std::span<char> resp_payload) const noexcept -> Result<std::size_t>;

private:
    enum Role { kLeader, kFollower, kCandidate };

    kosio::sync::Mutex      mutex_;
    std::atomic<bool>       is_shutdown_{false};
    std::atomic<uint64_t>   last_reset_time_{0};
    detail::Transport       transport_;
    std::atomic<Role>       role_{kFollower};
    std::optional<uint64_t> leader_id_{std::nullopt};
    detail::StateMachine    state_machine_;

    /* RaftState from https://raft.github.io/raft.pdf */
    // Persistent state on all servers
    std::atomic<uint64_t>   current_term_;
    std::optional<uint64_t> voted_for_;
    detail::RaftLog         logs_;

    // Volatile state on all servers
    uint64_t                commit_index_{0};
    uint64_t                last_applied_{0};

    // Volatile state on leaders
    std::unordered_map<uint64_t, uint64_t> next_index_{};
    std::unordered_map<uint64_t, uint64_t> match_index_{};
};
} // namespace foskv::raft