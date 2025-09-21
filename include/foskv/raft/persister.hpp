#pragma once
#include "foskv/raft/raft_config.hpp"
#include <vector>

namespace foskv::raft::detail {
class Persister {
private:
    explicit Persister(storage::Storage&& st);

public:
    Persister(Persister&& other) noexcept;
    auto operator=(Persister&& other) noexcept -> Persister&;

public:
    static auto create(std::string_view data_dir) -> Result<Persister>;

public:
    [[nodiscard]]
    auto persist_log_entry(const rocksdb::Slice& index_slice, const rocksdb::Slice &entry_payload_slice) const -> Result<void>;
    [[nodiscard]]
    auto persist_log_entries(const std::unordered_map<uint64_t, rocksdb::Slice>& entries) const -> Result<void>;
    [[nodiscard]]
    auto persist_state(const rocksdb::Slice& state_payload) const -> Result<void>;
    [[nodiscard]]
    auto persist_state(PersistState&& state) const -> Result<void>;
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