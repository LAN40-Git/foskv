#pragma once
#include <cstdint>
#include <vector>
#include <optional>
#include <kosio/sync.hpp>
#include <kosio/core.hpp>
#include "foskv/raft/raft.pb.h"

namespace foskv::raft {
struct RaftState {
    // RaftState from https://raft.github.io/raft.pdf
    // Persistent state on all servers
    // TODO: Read from disk
    uint64_t                current_term_{0};
    std::optional<uint64_t> voted_for_{std::nullopt};
    std::vector<LogEntry>   logs_{};

    // Volatile state on all servers
    uint64_t commit_index_{0};
    uint64_t last_applied_{0};

    // Volatile state on leaders
    std::vector<int> next_index_{};
    std::vector<int> match_index_{};
};

class RaftNode {
public:

public:
    auto election_event_loop() -> kosio::async::Task<>;
    auto heartbeat_event_loop() -> kosio::async::Task<>;

public:
    void do_election();
    void do_heartbeat();

public:
    // raft rpc
    void handle_request_vote_request(std::string_view payload, std::string& response);
    void handle_append_entries_request(std::string_view payload, std::string& response);
    void handle_install_snapshot_request(std::string_view payload, std::string& response);

private:
    RaftState          state_;
    uint64_t           last_rpc_time_{0};
    kosio::sync::Mutex mutex_;
};
} // namespace foskv::raft