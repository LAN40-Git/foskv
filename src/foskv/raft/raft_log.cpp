#include "foskv/raft/raft_log.hpp"

auto foskv::raft::detail::RaftLog::last_log_index()
const noexcept -> std::size_t {
    // Log index start from 1, return 0 means empty
    return entries_.size();
}

auto foskv::raft::detail::RaftLog::last_log_term()
const noexcept -> std::size_t {
    auto index = last_log_index();
    return index == 0 ? 0 : entries_[index].term();
}

auto foskv::raft::detail::RaftLog::entry_at(std::size_t index)
const noexcept -> std::optional<LogEntry> {
    if (index > entries_.size() || index < START_INDEX) {
        return std::nullopt;
    }
    return entries_[index];
}

void foskv::raft::detail::RaftLog::append(std::span<const LogEntry> entries) {
    entries_.insert(entries_.end(), entries.begin(), entries.end());
}
