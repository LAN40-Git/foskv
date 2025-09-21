#include "foskv/raft/raft_log.hpp"

foskv::raft::detail::RaftLog::RaftLog(
    uint64_t first_index,
    std::vector<LogEntry>&& entries,
    Persister &&persister)
    : first_index_(first_index)
    , entries_(std::move(entries))
    , persister_(std::move(persister)) {}

auto foskv::raft::detail::RaftLog::create(std::string_view data_dir) -> Result<RaftLog> {
    auto has_persister = Persister::create(data_dir);
    if (!has_persister) {
        return std::unexpected{has_persister.error()};
    }
    auto persister = std::move(has_persister.value());
    // Recover from disk
    auto has_entries = persister.recover_entries();
    if (!has_entries) {
        return std::unexpected{has_entries.error()};
    }
    auto entries = std::move(has_entries.value());
    uint64_t first_index = entries.empty() ? 1 : entries.front().index();
    return RaftLog{first_index, std::move(has_entries.value()), std::move(persister)};
}

auto foskv::raft::detail::RaftLog::last_log_index()
const noexcept -> std::size_t {
    return first_index_ - 1 + entries_.size();
}

auto foskv::raft::detail::RaftLog::last_log_term()
const noexcept -> std::size_t {
    return entries_.empty() ? 0 : entries_.back().term();
}

auto foskv::raft::detail::RaftLog::prev_log_index()
const noexcept -> std::size_t {
    return entries_.empty() ? 0 : last_log_index() - 1;
}

auto foskv::raft::detail::RaftLog::prev_log_term()
const noexcept -> std::size_t {
    if (entries_.empty()) {
        return 0;
    }
    if (entries_.size() == 1) {
        // TODO: Load from snapshot metadata
        // return first_index_ == 1 ? 0 : ;
    }
    return entries_[entries_.size() - 2].term();
}

auto foskv::raft::detail::RaftLog::entry_at(std::size_t index)
const noexcept -> Result<LogEntry> {
    if (index < first_index_ || index > entries_.size()) {
        return std::unexpected{make_error(Error::kInvalidLogIndex)};
    }
    return entries_[index-first_index_];
}

// void foskv::raft::detail::RaftLog::append(std::span<const LogEntry> entries) {
//     entries_.insert(entries_.end(), entries.begin(), entries.end());
// }
//
// void foskv::raft::detail::RaftLog::truncate(std::size_t start_index, std::size_t end_index) {
//     if (start_index < first_index_ || start_index > entries_.size()) {
//         return;
//     }
//     entries_.erase(entries_.begin() + (start_index - first_index_), entries_.end());
//     persister_.truncate_log(start_index, end_index);
// }
