#pragma once
#include "foskv/raft/transport.hpp"
#include "foskv/raft/persister.hpp"
#include <vector>
#include <optional>
#include <kosio/sync.hpp>
#include <kosio/core.hpp>

namespace foskv::raft {
class RaftNode {
private:
    explicit RaftNode(Config&& config, detail::Persister&& persister);

public:
    RaftNode(RaftNode&& other) noexcept;
    auto operator=(RaftNode&& other) noexcept -> RaftNode&;

public:
    [[REMEMBER_CO_AWAIT]]
    static auto create(const std::filesystem::path& config_path, const std::filesystem::path& data_dir)
    -> kosio::async::Task<RaftResult<RaftNode>>;

public:
    auto run() -> kosio::async::Task<>;

public:
    auto start_election_timeout() -> kosio::async::Task<>;
    auto start_heartbeat_timeout() -> kosio::async::Task<>;

public:
    void increase_term_to(uint64_t term);
    void become_leader();

public:
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
    auto handle_client_request(std::string_view req_payload, std::span<char> resp_payload)
    -> kosio::async::Task<RpcResult<std::size_t>>;

private:
    kosio::sync::Mutex mutex_;
    std::atomic<bool>  is_shutdown_{false};
    detail::Persister  persister_;
    detail::Transport  transport_;
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