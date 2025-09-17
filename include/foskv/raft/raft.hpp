#pragma once
#include <cstdint>
#include <vector>
#include <optional>
#include <kosio/sync.hpp>
#include <kosio/core.hpp>
#include "foskv/raft/raft.pb.h"
#include "foskv/raft/transport.hpp"

namespace foskv::raft {
struct RaftState {
    enum Role { kLeader, kFollower, kCandidate};
    Role role = kFollower;
    // RaftState from https://raft.github.io/raft.pdf
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

class RaftNode {
public:
    explicit RaftNode(std::string_view host, uint16_t port);
    explicit RaftNode(std::string_view host, uint16_t port, Transport::PeerMap&& peers);

public:
    auto election_event_loop() -> kosio::async::Task<>;
    auto heartbeat_event_loop() -> kosio::async::Task<>;

public:
    void do_election();
    void do_heartbeat();

public:
    // raft rpc callback
    auto request_vote_callback(RpcResult<std::string_view> has_response) -> kosio::async::Task<>;
    auto append_entries_callback(RpcResult<std::string_view> has_response) -> kosio::async::Task<>;
    auto install_snapshot_callback(RpcResult<std::string_view> has_response) -> kosio::async::Task<>;

private:
    kosio::sync::Mutex mutex_;
    RaftState          state_;
    Transport          transport_;
    uint64_t           last_rpc_time_{0};
};
} // namespace foskv::raft