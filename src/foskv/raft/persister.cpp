#include "foskv/raft/persister.hpp"

foskv::raft::detail::Persister::Persister(Persister &&other) noexcept
    : st_(std::move(other.st_))
    , buffer_(std::move(other.buffer_)) {}

auto foskv::raft::detail::Persister::operator=(Persister &&other) noexcept -> Persister & {
    st_ = std::move(other.st_);
    buffer_ = std::move(other.buffer_);
    return *this;
}

auto foskv::raft::detail::Persister::create(std::string_view data_dir)
-> Result<Persister> {
    rocksdb::Options options;
    options.create_if_missing = true;
    std::filesystem::path path(data_dir);
    path = path / RAFT_LOG_PATH;
    auto has_st = storage::Storage::Open(options, path);
    if (!has_st) [[unlikely]] {
        return std::unexpected{has_st.error()};
    }
    return Persister{std::move(has_st.value())};
}

auto foskv::raft::detail::Persister::persist(const rocksdb::Slice &index,
    const rocksdb::Slice &payload) const -> Result<void> {
    rocksdb::WriteBatch batch;
    batch.Put(index, payload);
    batch.Put(LAST_INDEX_KEY, index);
    if (auto status = st_.BatchWrite(batch); !status.ok()) {
        LOG_ERROR("Batch write failed: {}", status.ToString());
        return std::unexpected{make_error(Error::kLogEntryPersistFailed)};
    }
    return {};
}

auto foskv::raft::detail::Persister::persist(uint64_t current_term,
    std::optional<uint64_t> voted_for) -> Result<void> {
    PersistState state;
    state.set_current_term(current_term);
    if (voted_for.has_value()) {
        state.set_voted_for(voted_for.value());
    }
    if (!state.SerializeToArray(buffer_.data(), state.ByteSizeLong())) {
        return std::unexpected{make_error(Error::kPersistStateSerializeFailed)};
    }
    return Result<void>{};
}

auto foskv::raft::detail::Persister::persist_batch(
    std::vector<std::pair<uint64_t, rocksdb::Slice>> batch) const -> Result<void> {
    rocksdb::WriteBatch wb;
    for (auto& [index, payload] : batch) {
        wb.Put(std::to_string(index), payload);
    }
    wb.Put(LAST_INDEX_KEY, std::to_string(batch.back().first));
    return st_.BatchWrite(wb).ok() ? Result<void>{}
    : std::unexpected{make_error(Error::kLogEntryPersistFailed)};
}

auto foskv::raft::detail::Persister::recover_state() const -> Result<PersistState> {
    std::string value;
    if (auto status = st_.Get(PERSIST_STATE_KEY, &value); status.ok()) {
        // Do nothing
    } else if (status.IsNotFound()) {
        // First start, return default value
        PersistState state;
        state.set_current_term(0);
        // Do not set `voted_for`
        return state;
    } else {
        // Error
        return std::unexpected{make_error(Error::kPersistStateGetFailed)};
    }
    PersistState state;
    if (!state.ParseFromString(value)) {
        return std::unexpected{make_error(Error::kPersistStateParseFailed)};
    }
    return state;
}

auto foskv::raft::detail::Persister::recover_entries() const -> Result<std::vector<LogEntry>> {
    std::string first_index_value, last_index_value;
    auto first_index_status = st_.Get(FIRST_INDEX_KEY, &first_index_value);
    auto last_index_status = st_.Get(LAST_INDEX_KEY, &last_index_value);

    if (first_index_status.ok() && last_index_status.ok()) {
        // Do nothing
    } else if (first_index_status.IsNotFound() && last_index_status.IsNotFound()) {
        // Empty entries
        // Put first index and last index
        first_index_value = "1";
        last_index_value = "0";
        rocksdb::WriteBatch wb;
        wb.Put(FIRST_INDEX_KEY, first_index_value);
        wb.Put(LAST_INDEX_KEY, last_index_value);
        if (auto status = st_.BatchWrite(wb); !status.ok()) {
            LOG_ERROR("Batch write failed: {}", status.ToString());
            return std::unexpected{make_error(Error::kLogEntryPersistFailed)};
        }
        return {};
    } else {
        LOG_ERROR("Failed to read log index: first_status={}, last_status={}",
                 first_index_status.ToString(), last_index_status.ToString());
        return std::unexpected{make_error(Error::kInvalidLogIndex)};
    }

    uint64_t first_index, last_index;
    if (first_index_value.empty() || last_index_value.empty()) {
        LOG_ERROR("Empty index value: first='{}', last='{}'", first_index_value, last_index_value);
        return std::unexpected{make_error(Error::kLogIndexToUllFailed)};
    }
    try {
        first_index = std::stoull(first_index_value);
        last_index = std::stoull(last_index_value);
    } catch (const std::exception& e) {
        LOG_ERROR("Failed to convert log index: {}", e.what());
        return std::unexpected{make_error(Error::kLogIndexToUllFailed)};
    }

    // Validate index range
    if (first_index == 0 || (last_index != 0 && first_index > last_index)) {
        LOG_ERROR("Invalid log index range: first={}, last={}", first_index, last_index);
        return std::unexpected{make_error(Error::kInvalidLogIndex)};
    }

    // Empty entries
    if (last_index == 0) {
        if (auto status = st_.Put(FIRST_INDEX_KEY, "1"); !status.ok()) {
            LOG_ERROR("Init first_index failed: {}", status.ToString());
        }
        return {};
    }

    // Preallocate keys and values
    std::vector<std::string> keys;
    keys.reserve(last_index - first_index + 1);
    for (uint64_t i = first_index; i <= last_index; ++i) {
        keys.push_back(std::to_string(i));
    }

    std::vector<std::string> values;
    auto status_vec = st_.MultiGet({keys.begin(), keys.end()}, values);

    // Check for errors
    for (std::size_t i = 0; i < status_vec.size(); ++i) {
        if (!status_vec[i].ok()) {
            LOG_ERROR("Failed to read log entry at index {}: {}", keys[i], status_vec[i].ToString());
            return std::unexpected{make_error(Error::kLogEntriesRecoverFailed)};
        }
    }

    // Parse entries
    std::vector<LogEntry> entries;
    entries.reserve(values.size());
    for (std::size_t i = 0; i < values.size(); ++i) {
        LogEntry entry;
        if (!entry.ParseFromString(values[i])) {
            LOG_ERROR("Failed to parse log entry at index {}", keys[i]);
            return std::unexpected{make_error(Error::kLogEntryParseFailed)};
        }
        entries.emplace_back(std::move(entry));
    }
    return entries;
}
