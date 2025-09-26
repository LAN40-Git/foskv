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
    std::filesystem::create_directories(path);
    auto has_st = storage::Storage::Open(options, path);
    if (!has_st) [[unlikely]] {
        return std::unexpected{has_st.error()};
    }
    return Persister{std::move(has_st.value())};
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
    if (!st_.Put(PERSIST_STATE_KEY, {buffer_.data(), state.ByteSizeLong()}).ok()) {
        return std::unexpected{make_error(Error::kPersistStatePutFailed)};
    }
    return Result<void>{};
}

auto foskv::raft::detail::Persister::persist(const LogEntry &entry) -> Result<void> {
    auto key = std::to_string(entry.index());
    if (!entry.SerializeToArray(buffer_.data(), entry.ByteSizeLong())) {
        return std::unexpected{make_error(Error::kLogEntrySerializeFailed)};
    }
    rocksdb::WriteBatch wb;
    wb.Put(key, {buffer_.data(), entry.ByteSizeLong()});
    wb.Put(LAST_INDEX_KEY, key);
    return st_.BatchWrite(wb).ok() ?
    Result<void>{} : std::unexpected{make_error(Error::kLogEntriesPersistFailed)};
}

auto foskv::raft::detail::Persister::persist_batch(std::span<const LogEntry> entries)
const -> Result<void> {
    if (entries.empty()) {
        return std::unexpected{make_error(Error::kLogEntriesPersistFailed)};
    }
    std::vector<std::pair<std::string, std::string>> entries_copy;
    entries_copy.reserve(entries.size());
    for (const auto &entry : entries) {
        entries_copy.emplace_back(std::make_pair(std::to_string(entry.index()), entry.SerializeAsString()));
    }
    rocksdb::WriteBatch wb;
    for (auto& [index, payload] : entries_copy) {
        wb.Put(index, payload);
    }
    wb.Put(LAST_INDEX_KEY, entries_copy.back().first);
    return st_.BatchWrite(wb).ok() ?
    Result<void>{} : std::unexpected{make_error(Error::kLogEntriesPersistFailed)};
}

auto foskv::raft::detail::Persister::truncate_batch(uint64_t start_index, uint64_t end_index) const -> Result<void> {
    std::vector<std::string> keys;
    rocksdb::WriteBatch wb;
    keys.reserve(end_index - start_index + 1);
    for (uint64_t i = start_index; i <= end_index; ++i) {
        keys.emplace_back(std::to_string(i));
        wb.Delete(keys[i]);
    }
    return st_.BatchWrite(wb).ok() ?
    Result<void>{} : std::unexpected{make_error(Error::kLogEntriesTruncateFailed)};
}

auto foskv::raft::detail::Persister::recover_state() const -> Result<PersistState> {
    std::string value;
    if (auto status = st_.Get(PERSIST_STATE_KEY, &value); status.ok()) {
        // Do nothing
    } else if (status.IsNotFound()) {
        LOG_INFO("Persist state not found.");
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
        // LOG_INFO("first_index : {}, last_index : {}", first_index_value, last_index_value);
    } else if (first_index_status.IsNotFound() && last_index_status.IsNotFound()) {
        LOG_INFO("first_index and last_index not found");
        // Empty entries
        // Put first index and last index
        first_index_value = "1";
        last_index_value = "0";
        rocksdb::WriteBatch wb;
        wb.Put(FIRST_INDEX_KEY, first_index_value);
        wb.Put(LAST_INDEX_KEY, last_index_value);
        if (auto status = st_.BatchWrite(wb); !status.ok()) {
            LOG_ERROR("Batch write failed: {}", status.ToString());
            return std::unexpected{make_error(Error::kLogEntriesPersistFailed)};
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
