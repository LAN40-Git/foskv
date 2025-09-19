#include "foskv/raft/persister.hpp"
#include <kosio/common/debug.hpp>

foskv::raft::detail::Persister::Persister(storage::Storage&& st)
    : st_(std::move(st))
    , keys_(128)
    , values_(128) {}

foskv::raft::detail::Persister::Persister(Persister &&other) noexcept
    : st_(std::move(other.st_))
    , keys_(std::move(other.keys_))
    , values_(std::move(other.values_)) {}

auto foskv::raft::detail::Persister::operator=(Persister &&other) noexcept -> Persister & {
    st_ = std::move(other.st_);
    keys_ = std::move(other.keys_);
    values_ = std::move(other.values_);
    return *this;
}

auto foskv::raft::detail::Persister::create(const std::filesystem::path& path)
-> RaftResult<Persister> {
    rocksdb::Options options;
    options.create_if_missing = true;
    auto has_st = storage::Storage::Open(options, path);
    if (!has_st) [[unlikely]] {
        LOG_ERROR("{}", has_st.error());
        return std::unexpected{make_raft_error(RaftError::kPersisterCreateFailed)};
    }
    return Persister{std::move(has_st.value())};
}

auto foskv::raft::detail::Persister::persist_entry(const std::pair<rocksdb::Slice, rocksdb::Slice> &entry) const -> RaftResult<void> {
    auto status = st_.Put(entry.first, entry.second);
    if (!status.ok()) [[unlikely]] {
        LOG_ERROR("Failed to persist entry {}-{} : {}", entry.first.ToString(), entry.second.ToString(), status.ToString());
        return std::unexpected{make_raft_error(RaftError::kPersistentSaveFailed)};
    }
    return RaftResult<void>{};
}

auto foskv::raft::detail::Persister::persist_entries(const std::vector<std::pair<rocksdb::Slice, rocksdb::Slice>>& entries)
const -> RaftResult<void> {
    rocksdb::WriteBatch write_batch;
    for (auto& entry : entries) {
        write_batch.Put(entry.first, entry.second);
    }
    auto status = st_.BatchWrite(write_batch);
    if (!status.ok()) [[unlikely]] {
        LOG_ERROR("Failed to persist entries : {}", status.ToString());
        return std::unexpected{make_raft_error(RaftError::kPersistentSaveFailed)};
    }
    return RaftResult<void>{};
}

auto foskv::raft::detail::Persister::persist_state(
    const rocksdb::Slice& state_payload) const -> RaftResult<void> {
    auto status = st_.Put(PERSISTENT_KEY, state_payload);
    if (!status.ok()) [[unlikely]] {
        LOG_ERROR("Failed to persist state : {}", status.ToString());
        return std::unexpected{make_raft_error(RaftError::kPersistentSaveFailed)};
    }
    return RaftResult<void>{};
}

void foskv::raft::detail::Persister::load_state(PersistState &state) const {
    std::string state_payload;
    if (!st_.Get(PERSISTENT_KEY, &state_payload).ok() ||
        !state.ParseFromString(state_payload)) [[unlikely]] {
        state.set_current_term(0);
        state.clear_voted_for();
    }
}

void foskv::raft::detail::Persister::load_entries(std::vector<LogEntry> &entries) {
    try {
        std::string start_log_index_str, end_log_index_str;

        entries = {LogEntry{}};

        if (!st_.Get(START_LOG_INDEX, &start_log_index_str).ok() ||
            !st_.Get(END_LOG_INDEX, &end_log_index_str).ok()) {
            return;
        }

        auto start_log_index = std::stoull(start_log_index_str);
        auto end_log_index = std::stoull(end_log_index_str);

        if (start_log_index > end_log_index) [[unlikely]] {
            throw std::invalid_argument("Invalid log index range");
        }

        auto entries_size = end_log_index - start_log_index + 1;
        entries.resize(entries_size);
        keys_.resize(entries_size);
        values_.resize(entries_size);

        // Store entry keys
        std::size_t count = 0;
        for (uint64_t i = start_log_index; i <= end_log_index; ++i) {
            keys_[count] = std::to_string(i);
            count += 1;
        }
        auto status_vec = st_.MultiGet(
            {keys_.begin(), keys_.begin() + entries_size}, values_);
        for (std::size_t i = 0; i < entries_size; ++i) {
            if (!status_vec[i].ok()) {
                LOG_ERROR("Failed to get entry at index {}", start_log_index + i);
                throw std::runtime_error("Entry data missing");
            }
            if (!entries[i].ParseFromString(values_[i])) [[unlikely]] {
                LOG_ERROR("Failed to parse entry at index {}", start_log_index + i);
                throw std::runtime_error("Entry parse failed");
            }
        }
        // Log recovery completes
    } catch (const std::exception& e) {
        LOG_ERROR("Failed to load entries: {}", e.what());
        entries = {LogEntry{}};
    }
    catch (...) {
        LOG_ERROR("Unknown error occurred while loading entries");
        entries = {LogEntry{}};
    }
}
