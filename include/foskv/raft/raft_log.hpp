#pragma once
#include "foskv/rpc.hpp"

namespace foskv::raft::detail {
class RaftLog {
    static constexpr std::size_t START_INDEX = 1;
public:
    [[nodiscard]] auto last_log_index() const noexcept -> std::size_t;
    [[nodiscard]] auto last_log_term() const noexcept -> std::size_t;
    [[nodiscard]] auto entry_at(std::size_t index) const noexcept -> std::optional<LogEntry>;

    void append(std::span<const LogEntry> entries);
    void truncate(std::size_t from_index);

private:
    std::vector<LogEntry> entries_{};
};
} // namespace foskv::raft::detail