#pragma once
#include "foskv/rpc.hpp"
#include "foskv/storage/storage.hpp"
#include "foskv/raft/config.hpp"

namespace foskv::raft::detail {
class Persister {
private:
    explicit Persister(storage::Storage&& st)
        : st_(std::move(st)) {}

public:
    // Delete copy
    Persister(const Persister&) = delete;
    auto operator=(const Persister&) -> Persister& = delete;

    Persister(Persister&& other) noexcept
        : st_(std::move(other.st_)) {}
    auto operator=(Persister&& other) noexcept -> Persister& {
        st_ = std::move(other.st_);
        return *this;
    }

public:
    static auto create(std::string_view data_dir) -> Result<Persister> {
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

public:
    auto persist(const rocksdb::Slice& index, const rocksdb::Slice& payload) const -> Result<void> {
        auto status = st_.Put(index, payload);
        if (!status.ok()) [[unlikely]] {
            LOG_ERROR("{}", status.ToString());
            return std::unexpected{make_error(Error::kLogEntryPersistFailed)};
        }
        return Result<void>{};
    }

    auto persist_batch(std::vector<std::pair<uint64_t, rocksdb::Slice>> batch) const -> Result<void> {
        rocksdb::WriteBatch wb;
        for (auto& [index, payload] : batch) {
            wb.Put(std::to_string(index), payload);
        }
        auto status = st_.BatchWrite(wb);
        if (!status.ok()) {
            LOG_ERROR("{}", status.ToString());
            return std::unexpected{make_error(Error::kLogEntryPersistFailed)};
        }
        return Result<void>{};
    }

    auto recover(uint64_t offset, uint64_t size) const -> Result<std::vector<LogEntry>> {
        std::vector<LogEntry> entries;
        std::vector<std::string> keys;
        std::vector<std::string> values;
        for (uint64_t i = offset; i < offset + size; ++i) {
            keys.emplace_back(std::to_string(i));
        }
        auto status_vec = st_.MultiGet({keys.begin(), keys.end()}, values);
        // Check if error
        for (std::size_t i = 0; i < size; ++i) {
            if (!status_vec[i].ok()) {
                LOG_ERROR("{}", status_vec[i].ToString());
                return std::unexpected{make_error(Error::kLogEntriesRecoverFailed)};
            }
            LogEntry entry;
            if (!entry.ParseFromString(values[i])) {
                LOG_ERROR("Failed to parse from log entry payload");
                return std::unexpected{make_error(Error::kLogEntryParseFailed)};
            }
            entries.emplace_back(std::move(entry));
        }
        // Successfully recover from storage
        return entries;
    }

private:
    storage::Storage st_;
};
} // namespace foskv::raft::detail