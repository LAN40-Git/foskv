#pragma once
#include "foskv/raft/peer.hpp"

namespace foskv::raft {
class RaftNode;
}

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
    auto apply(RaftNode& node, const LogEntry& entry) const -> kosio::async::Task<RaftResult<void>>;

private:
    storage::Storage st_;
};
} // namespace foskv::raft::detail