#pragma once
#include "foskv/raft/persister.hpp"

namespace foskv::raft::detail {
class RaftLog {
private:
    explicit RaftLog(uint64_t first_index, std::vector<LogEntry>&& entries, Persister&& persister);

public:
    RaftLog(const RaftLog&) = delete;
    auto operator=(const RaftLog&) -> RaftLog& = delete;
    RaftLog(RaftLog&&) = default;
    auto operator=(RaftLog&&) -> RaftLog& = default;

public:
    static auto create(std::string_view data_dir) -> Result<RaftLog>;

public:
    [[nodiscard]] auto last_log_index() const noexcept -> std::size_t;
    [[nodiscard]] auto last_log_term() const noexcept -> std::size_t;
    [[nodiscard]] auto prev_log_index() const noexcept -> std::size_t;
    [[nodiscard]] auto prev_log_term() const noexcept -> std::size_t;
    /// @return Return LogEntry whose index is index
    [[nodiscard]] auto entry_at(std::size_t index) const noexcept -> Result<LogEntry>;
    void append(std::span<const LogEntry> entries);
    void truncate(std::size_t start_index, std::size_t end_index);

private:
    uint64_t              first_index_;
    std::vector<LogEntry> entries_;
    Persister             persister_;
};
} // namespace foskv::raft::detail