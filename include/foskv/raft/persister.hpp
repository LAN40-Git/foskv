#pragma once
#include "foskv/raft/raft.pb.h"
#include "foskv/storage/storage.hpp"
#include <string_view>

namespace foskv::raft::detail {
class Persister {
    static constexpr std::string_view PERSISTENT_KEY = "P";
    static constexpr std::string_view START_LOG_INDEX = "S";
    static constexpr std::string_view END_LOG_INDEX = "E";
public:
    explicit Persister(storage::Storage st);
    Persister(Persister&& other) noexcept;
    auto operator=(Persister&& other) noexcept -> Persister&;

public:
    static auto create(std::string_view persistent_path) -> RaftResult<Persister>;

public:
    [[nodiscard]]
    auto persist_entry(const std::pair<rocksdb::Slice, rocksdb::Slice> &entry) const -> RaftResult<void>;

    [[nodiscard]]
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