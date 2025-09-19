#pragma once
#include "foskv/raft/peer.hpp"

namespace foskv::raft::detail {
class StateMachine {
private:
    explicit StateMachine(storage::Storage&& st);

public:
    StateMachine(StateMachine&& other) noexcept;
    auto operator=(StateMachine&& other) noexcept -> StateMachine&;

public:
    static auto create(const std::filesystem::path& path) -> RaftResult<StateMachine>;

public:
    auto apply_entries(std::span<const LogEntry> entries) -> uint64_t;

private:
    storage::Storage st_;
};
} // namespace foskv::raft::detail