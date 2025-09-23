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
    return RaftLog{first_index, std::move(entries), std::move(persister)};
}

auto foskv::raft::detail::RaftLog::last_log_index()
const noexcept -> uint64_t {
    return first_index_ + entries_.size() - 1;
}

auto foskv::raft::detail::RaftLog::last_log_term()
const noexcept -> uint64_t {
    if (entries_.empty() && last_log_index() == 0) {
        return 0;
    }
    if (entries_.empty() && last_log_index() > 0) {
        // TODO: Load from snapshot metadata

    }
    return entries_.back().term();
}

auto foskv::raft::detail::RaftLog::prev_log_index()
const noexcept -> uint64_t {
    return entries_.empty() ? 0 : last_log_index() - 1;
}

auto foskv::raft::detail::RaftLog::prev_log_term()
const noexcept -> uint64_t {
    if (entries_.empty()) {
        return 0;
    }
    if (entries_.size() == 1) {
        // TODO: Load from snapshot metadata
        return first_index_ == 1 ? 0 : 0;
    }
    return entries_[entries_.size() - 2].term();
}

auto foskv::raft::detail::RaftLog::entry_at(uint64_t index)
const noexcept -> Result<LogEntry> {
    if (index < first_index_ || index > last_log_index()) {
        return std::unexpected{make_error(Error::kInvalidLogIndex)};
    }
    return entries_[index-first_index_];
}

auto foskv::raft::detail::RaftLog::term_at(uint64_t index)
const noexcept -> uint64_t {
    if (index < first_index_ || index > last_log_index()) {
        return 0;
    }
    return entries_[index-first_index_].term();
}

auto foskv::raft::detail::RaftLog::append_entry(LogEntry &&entry) -> Result<void> {
    if (auto ret = persister_.persist(entry); !ret) {
        return std::unexpected{ret.error()};
    }
    entries_.emplace_back(std::move(entry));
    return Result<void>{};
}

auto foskv::raft::detail::RaftLog::append_entries(std::vector<LogEntry>&& entries) -> Result<void> {
    // auto start_index = entries_.size();
    // auto entries_size = entries.size();

    if (auto ret = persister_.persist_batch(entries); !ret) {
        return std::unexpected{ret.error()};
    }

    entries_.insert(
        entries_.end(),
        std::make_move_iterator(entries.begin()),
        std::make_move_iterator(entries.end())
    );
    return Result<void>{};

    // return std::span<const LogEntry>(
    //     entries_.data() + start_index,
    //     entries_size
    // );
}

void foskv::raft::detail::RaftLog::truncate_entries(uint64_t start_index) {
    if (start_index < first_index_ || start_index > last_log_index()) {
        return;
    }
    entries_.erase(entries_.begin() + (start_index - first_index_), entries_.end());
    auto has_truncate = persister_.truncate_batch(start_index, last_log_index());
    // TODO: Handle this
    if (!has_truncate) {
        LOG_FATAL("Failed to truncate entries.");
    }
}
