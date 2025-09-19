#pragma once
#include "foskv/raft/config.hpp"

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
    // pair : [index, payload]
    auto persist_entry(const std::pair<rocksdb::Slice, rocksdb::Slice> &entry) const -> RaftResult<void>;
    [[nodiscard]]
    // pair : [index, payload]
    auto persist_entries(const std::vector<std::pair<rocksdb::Slice, rocksdb::Slice>>& entries) const -> RaftResult<void>;
    [[nodiscard]]
    auto persist_state(const rocksdb::Slice& state_payload) const -> RaftResult<void>;
    void load_state(PersistState& state) const;
    void load_entries(std::vector<LogEntry>& entries);

private:
    storage::Storage st_;
    std::vector<std::string> keys_;
    std::vector<std::string> values_;
};
} // namespace foskv::raft::detail