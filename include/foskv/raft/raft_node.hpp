#pragma once
#include "foskv/raft/transport.hpp"
#include <vector>
#include <optional>
#include <kosio/sync.hpp>
#include <kosio/core.hpp>

namespace foskv::raft {
class RaftNode {
public:
    explicit RaftNode(uint64_t member_id, const kosio::net::SocketAddr &addr);
    explicit RaftNode(uint64_t member_id, const kosio::net::SocketAddr &addr, Transport::PeerMap&& peers);

public:
    void init();
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

private:
    std::atomic<bool>     is_shutdown_{false};
    kosio::sync::Mutex    mutex_;
    Transport             transport_;
    // election timeout last reset time ms
    std::atomic<uint64_t> last_reset_time_{0};

    /* RaftState from https://raft.github.io/raft.pdf */
    enum Role { kLeader, kFollower, kCandidate};
    std::atomic<Role> role_ = kFollower;
    // Persistent state on all servers
    // TODO: Read from disk
    std::atomic<uint64_t>   current_term_{0};
    std::optional<uint64_t> voted_for_{std::nullopt};
    // Default one entry
    std::vector<LogEntry>   logs_{LogEntry{}};

    // Volatile state on all servers
    uint64_t                commit_index_{0};
    uint64_t                last_applied_{0};

    // Volatile state on leaders
    std::vector<int> next_index_{0};
    std::vector<int> match_index_{0};
};
} // namespace foskv::raft