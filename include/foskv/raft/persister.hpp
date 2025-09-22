#pragma once
#include "foskv/rpc.hpp"
#include "foskv/storage/storage.hpp"
#include "foskv/raft/config.hpp"

namespace foskv::raft::detail {
class RaftLog;

class Persister {
    friend class RaftLog;
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
    [[nodiscard]]
    auto persist(uint64_t current_term, std::optional<uint64_t> voted_for) -> Result<void>;
    [[nodiscard]]
    auto persist_batch(std::span<const LogEntry> entries) const -> Result<void>;
    [[nodiscard]]
    auto truncate_batch(uint64_t start_index, uint64_t end_index) const -> Result<void>;
    [[nodiscard]]
    auto recover_state() const -> Result<PersistState>;

private:
    [[nodiscard]]
    // RaftNode does not need to call this method
    auto recover_entries() const -> Result<std::vector<LogEntry>>;

private:
    storage::Storage  st_;
    std::vector<char> buffer_;
};
} // namespace foskv::raft::detail