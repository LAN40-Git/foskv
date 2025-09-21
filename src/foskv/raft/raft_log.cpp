#include "foskv/raft/raft_log.hpp"

auto foskv::raft::detail::RaftLog::create(std::string_view data_dir,
    uint64_t offset, uint64_t size) -> Result<RaftLog> {
    auto has_persister = Persister::create(data_dir);
    if (!has_persister) {
        return std::unexpected{has_persister.error()};
    }
    // Recover from disk
}

auto foskv::raft::detail::RaftLog::last_log_index()
const noexcept -> std::size_t {
    assert(start_index_ >= 1);
    return start_index_ - 1 + entries_.size();
}

auto foskv::raft::detail::RaftLog::last_log_term()
const noexcept -> std::size_t {
    auto index = last_log_index();
    return index == 0 ? 0 : entries_[index].term();
}

auto foskv::raft::detail::RaftLog::entry_at(std::size_t index)
const noexcept -> LogEntry {
    assert(index >= start_index_ && index <= entries_.size());
    return entries_[index];
}

void foskv::raft::detail::RaftLog::append(std::span<const LogEntry> entries) {
    entries_.insert(entries_.end(), entries.begin(), entries.end());
}

void foskv::raft::detail::RaftLog::truncate(std::size_t from_index) {
    assert(from_index >= start_index_ && from_index <= entries_.size());
    entries_.erase(entries_.begin() + from_index - 1, entries_.end());


}
