#pragma once
#include "foskv/raft/config.hpp"
#include <vector>

namespace foskv::raft::detail {
class Persister {
private:
    explicit Persister(storage::Storage&& st);

public:
    Persister(Persister&& other) noexcept;
    auto operator=(Persister&& other) noexcept -> Persister&;

public:
    static auto create(const std::filesystem::path& path) -> RaftResult<Persister>;

public:
    [[nodiscard]]
    auto persist_entry(const rocksdb::Slice& index_slice, const rocksdb::Slice &entry_payload_slice) const -> RaftResult<void>;
    [[nodiscard]]
    auto persist_entries(const std::unordered_map<uint64_t, rocksdb::Slice>& entries) const -> RaftResult<void>;
    [[nodiscard]]
    auto persist_state(const rocksdb::Slice& state_payload) const -> RaftResult<void>;
    [[nodiscard]]
    auto persist_state(PersistState&& state) const -> RaftResult<void>;
    [[nodiscard]]
    auto load_state() const -> PersistState;
    [[nodiscard]]
    auto load_entries() -> std::vector<LogEntry>;

private:
    storage::Storage st_;
    std::vector<std::string> keys_;
    std::vector<std::string> values_;
};
} // namespace foskv::raft::detail