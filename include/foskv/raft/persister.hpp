#pragma once
#include "foskv/rpc.hpp"
#include "foskv/storage/storage.hpp"
#include "foskv/raft/config.hpp"

namespace foskv::raft::detail {
class Persister {
private:
    explicit Persister(storage::Storage&& st)
        : st_(std::move(st))
        , buffer_(128) {}

public:
    // Delete copy
    Persister(const Persister&) = delete;
    auto operator=(const Persister&) -> Persister& = delete;

    Persister(Persister&& other) noexcept;
    auto operator=(Persister&& other) noexcept -> Persister&;

public:
    static auto create(std::string_view data_dir) -> Result<Persister>;

public:
    auto persist(const rocksdb::Slice& index, const rocksdb::Slice& payload) const -> Result<void>;
    auto persist(uint64_t current_term, std::optional<uint64_t> voted_for) -> Result<void>;
    auto persist_batch(std::vector<std::pair<uint64_t, rocksdb::Slice>> batch) const -> Result<void>;
    auto recover_state() const -> Result<PersistState>;
    auto recover_entries() const -> Result<std::vector<LogEntry>>;
    auto remove_entries(std::size_t start_index, std::size_t end_index);

private:
    storage::Storage  st_;
    std::vector<char> buffer_;
};
} // namespace foskv::raft::detail