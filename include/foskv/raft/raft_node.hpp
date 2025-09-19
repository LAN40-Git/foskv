#pragma once
#include "foskv/raft/transport.hpp"
#include "foskv/raft/persister.hpp"
#include "foskv/raft/state_machine.hpp"

namespace foskv::raft {
class RaftNode {
    friend class detail::StateMachine;
private:
    explicit RaftNode(Config&& config, detail::Persister&& persister, detail::StateMachine&& state_machine);

public:
    RaftNode(RaftNode&& other) noexcept;
    auto operator=(RaftNode&& other) noexcept -> RaftNode&;

public:
    [[REMEMBER_CO_AWAIT]]
    static auto create(const std::filesystem::path& config_path, const std::filesystem::path& data_dir)
    -> kosio::async::Task<RaftResult<RaftNode>>;

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
    -> kosio::async::Task<RpcResult<std::size_t>>;
    [[REMEMBER_CO_AWAIT]]
    auto handle_append_entries_request(std::string_view req_payload, std::span<char> resp_payload)
    -> kosio::async::Task<RpcResult<std::size_t>>;
    [[REMEMBER_CO_AWAIT]]
    auto handle_install_snapshot_request(std::string_view req_payload, std::span<char> resp_payload)
    -> kosio::async::Task<RpcResult<std::size_t>>;
    [[REMEMBER_CO_AWAIT]]
    auto handle_internal_raft_request(std::string_view req_payload, std::span<char> resp_payload)
    -> kosio::async::Task<RpcResult<std::size_t>>;

private:
    /* Confirm that you have the lock */
    auto generate_response_header() const noexcept -> ResponseHeader;
    auto generate_internal_response_header(bool success, int error_code = 0,
        std::optional<rpc::Redirect> redirect = std::nullopt) const noexcept -> rpc::ResponseHeader;
    auto produce_append_entries_request() const noexcept -> AppendEntriesRequest;

private:
    kosio::sync::Mutex mutex_;
    std::atomic<bool>  is_shutdown_{false};
    detail::Persister  persister_;
    detail::Transport  transport_;
    detail::StateMachine state_machine_;
    std::array<char, 2*rpc::detail::MAX_RPC_MESSAGE_SIZE> buffer_{};
    // election timeout last reset time ms
    std::atomic<uint64_t> last_reset_time_{0};

    /* RaftState from https://raft.github.io/raft.pdf */
    enum Role { kLeader, kFollower, kCandidate};
    std::atomic<Role> role_ = kFollower;
    // Persistent state on all servers
    std::atomic<uint64_t>   current_term_;
    std::optional<uint64_t> voted_for_;
    std::vector<LogEntry>   logs_{};

    // Volatile state on all servers
    uint64_t                commit_index_{0};
    uint64_t                last_applied_{0};

    // Volatile state on leaders
    std::optional<uint64_t> leader_id_{std::nullopt};
    std::vector<int> next_index_{};
    std::vector<int> match_index_{};
};
} // namespace foskv::raft