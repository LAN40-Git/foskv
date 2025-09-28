#pragma once
#include "foskv/raft/raft_log.hpp"
#include "foskv/raft/state_machine.hpp"

namespace foskv::raft {
class RaftNode {
    friend class detail::StateMachine;
public:
    explicit RaftNode(detail::Transport&& transport, detail::StateMachine&& state_machine,
        PersistState&& state, detail::RaftLog&& logs);

public:
    static auto create(std::string_view config_path, std::string_view data_dir) -> kosio::async::Task<Result<std::unique_ptr<RaftNode>>>;

public:
    [[REMEMBER_CO_AWAIT]]
    auto run() -> kosio::async::Task<Result<void>>;
    [[REMEMBER_CO_AWAIT]]
    auto shutdown() -> kosio::async::Task<>;

private:
    auto start_election_timeout() -> kosio::async::Task<>;
    auto start_heartbeat_timeout() -> kosio::async::Task<>;
    // auto start_commit_timeout() -> kosio::async::Task<>;

private:
    void start_election();
    void increase_term_to(uint64_t term);
    void become_leader();

private:
    [[REMEMBER_CO_AWAIT]]
    auto handle_request_vote_response(std::string_view resp_payload) -> kosio::async::Task<>;
    [[REMEMBER_CO_AWAIT]]
    auto handle_heartbeat_response(std::string_view resp_payload) -> kosio::async::Task<>;
    [[REMEMBER_CO_AWAIT]]
    auto handle_append_entries_response(std::string_view resp_payload) -> kosio::async::Task<>;
    [[REMEMBER_CO_AWAIT]]
    auto handle_install_snapshot_response(std::string_view resp_payload) -> kosio::async::Task<>;

private:
    // raft rpc invoke
    [[REMEMBER_CO_AWAIT]]
    auto handle_request_vote_request(
        std::string_view req_payload, std::span<char> resp_payload,
        uint64_t session_id = 0, uint64_t request_id = 0) -> kosio::async::Task<Result<std::size_t>>;
    [[REMEMBER_CO_AWAIT]]
    auto handle_append_entries_request(
        std::string_view req_payload, std::span<char> resp_payload,
        uint64_t session_id = 0, uint64_t request_id = 0) -> kosio::async::Task<Result<std::size_t>>;
    [[REMEMBER_CO_AWAIT]]
    auto handle_install_snapshot_request(std::string_view req_payload, std::span<char> resp_payload,
        uint64_t session_id = 0, uint64_t request_id = 0) -> kosio::async::Task<Result<std::size_t>>;

    // business rpc invoke
    [[REMEMBER_CO_AWAIT]]
    auto handle_kv_put_request(std::string_view req_payload, std::span<char> resp_payload,
        uint64_t session_id, uint64_t request_id)
    -> kosio::async::Task<Result<std::size_t>>;
    [[REMEMBER_CO_AWAIT]]
    auto handle_kv_get_request(std::string_view req_payload, std::span<char> resp_payload,
        uint64_t session_id, uint64_t request_id)
    -> kosio::async::Task<Result<std::size_t>>;
    [[REMEMBER_CO_AWAIT]]
    auto handle_kv_delete_request(std::string_view req_payload, std::span<char> resp_payload,
        uint64_t session_id, uint64_t request_id)
    -> kosio::async::Task<Result<std::size_t>>;

private:
    /* For raft */
    [[nodiscard]]
    auto produce_response_header() const noexcept -> ResponseHeader;
    [[nodiscard]]
    auto produce_request_vote_request() const noexcept -> RequestVoteRequest;
    [[nodiscard]]
    auto produce_request_vote_response(std::span<char> resp_payload, bool vote_granted = true) const noexcept -> Result<std::size_t>;
    [[nodiscard]]
    auto produce_append_entries_request() const noexcept -> AppendEntriesRequest;
    [[nodiscard]]
    auto produce_append_entries_response(std::span<char> resp_payload, bool success = true, uint64_t conflict_index = 1) const noexcept -> Result<std::size_t>;
    [[nodiscard]]
    auto produce_install_snapshot_request(uint64_t last_included_index, uint64_t last_included_term,
        uint64_t offset, std::string&& data, bool done) const noexcept -> InstallSnapshotRequest;
    [[nodiscard]]
    auto produce_install_snapshot_response(std::span<char> resp_payload) const noexcept -> Result<std::size_t>;

private:
    enum Role { kLeader, kFollower, kCandidate };

    kosio::sync::Mutex      mutex_;
    kosio::sync::Latch      latch_{2};
    std::atomic<bool>       is_shutdown_{false};
    std::atomic<uint64_t>   last_reset_time_{0};
    detail::Transport       transport_;
    detail::Proposer        proposer_;
    std::atomic<Role>       role_{kFollower};
    std::size_t             votes_{0};
    std::optional<uint64_t> leader_id_{std::nullopt};
    detail::StateMachine    state_machine_;

    /* RaftState from https://raft.github.io/raft.pdf */
    // Persistent state on all servers
    std::atomic<uint64_t>                     current_term_;
    std::optional<uint64_t>                   voted_for_;
    detail::RaftLog                           logs_;

    // Volatile state on all servers
    uint64_t                commit_index_{0};
    uint64_t                last_applied_{0};

    // Volatile state on leaders
    std::unordered_map<uint64_t, uint64_t> next_index_{};
    std::unordered_map<uint64_t, uint64_t> match_index_{};
};
} // namespace foskv::raft