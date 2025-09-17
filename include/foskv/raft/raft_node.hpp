#pragma once
#include <cstdint>
#include <vector>
#include <optional>
#include <kosio/sync.hpp>
#include <kosio/core.hpp>
#include "foskv/raft/transport.hpp"

namespace foskv::raft {
class RaftNode {
public:
    explicit RaftNode(std::string_view host, uint16_t port);
    explicit RaftNode(std::string_view host, uint16_t port, Transport::PeerMap&& peers);

public:
    auto start_election_timeout() -> kosio::async::Task<>;
    auto start_heartbeat_timeout() -> kosio::async::Task<>;

public:
    void increase_term_to(uint64_t term);

public:
    // raft rpc invoke
    auto handle_request_vote_request() -> RpcResult<std::size_t>;
    auto handle_append_entries_request() -> RpcResult<std::size_t>;
    auto handle_install_snapshot_request() -> RpcResult<std::size_t>;

private:
    std::atomic<bool>  is_shutdown_{false};
    kosio::sync::Mutex mutex_;
    Transport          transport_;
    uint64_t           last_rpc_time_{0};

    /* RaftState from https://raft.github.io/raft.pdf */
    enum Role { kLeader, kFollower, kCandidate};
    Role role = kFollower;
    // Persistent state on all servers
    // TODO: Read from disk
    uint64_t                current_term{0};
    std::optional<uint64_t> voted_for{std::nullopt};
    std::vector<LogEntry>   logs{};

    // Volatile state on all servers
    uint64_t commit_index{0};
    uint64_t last_applied{0};

    // Volatile state on leaders
    std::vector<int> next_index{};
    std::vector<int> match_index{};
};
} // namespace foskv::raft