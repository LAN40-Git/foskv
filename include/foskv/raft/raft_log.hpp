#pragma once
#include "foskv/raft/persister.hpp"

namespace foskv::raft::detail {
class RaftLog {
private:
    explicit RaftLog(uint64_t start_index, std::vector<LogEntry>&& entries, Persister&& persister)
        : start_index_(start_index)
        , entries_(std::move(entries))
        , persister_(std::move(persister)) {}

public:
    RaftLog(const RaftLog&) = delete;
    auto operator=(const RaftLog&) -> RaftLog& = delete;
    RaftLog(RaftLog&&) = default;
    auto operator=(RaftLog&&) -> RaftLog& = default;

public:
    static auto create(std::string_view data_dir, uint64_t offset, uint64_t size) -> Result<RaftLog>;

public:
    [[nodiscard]] auto last_log_index() const noexcept -> std::size_t;
    [[nodiscard]] auto last_log_term() const noexcept -> std::size_t;
    [[nodiscard]] auto entry_at(std::size_t index) const noexcept -> LogEntry;

    void append(std::span<const LogEntry> entries);
    void truncate(std::size_t from_index);

private:
    uint64_t              start_index_{1};
    std::vector<LogEntry> entries_;
    Persister             persister_;
};
} // namespace foskv::raft::detail