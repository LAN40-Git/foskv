#pragma once
#include "foskv/raft/persister.hpp"

namespace foskv::raft {
class RaftNode;
} // namespace foskv::raft

namespace foskv::raft::detail {
class RaftLog {
    friend class foskv::raft::RaftNode;
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
    [[nodiscard]] auto last_log_index() const noexcept -> uint64_t;
    [[nodiscard]] auto last_log_term() const noexcept -> uint64_t;
    [[nodiscard]] auto prev_log_index() const noexcept -> uint64_t;
    [[nodiscard]] auto prev_log_term() const noexcept -> uint64_t;
    [[nodiscard]] auto entry_at(uint64_t index) const noexcept -> Result<LogEntry>;
    [[nodiscard]] auto term_at(uint64_t index) const noexcept -> uint64_t;
    [[nodiscard]] auto append_entry(LogEntry&& entry) -> Result<void>;
    [[nodiscard]] auto append_entries(std::vector<LogEntry>&& entries) -> Result<void>;
    void truncate_entries(uint64_t start_index);

public:
    // For test
    auto recover_state_test() const -> Result<PersistState> {
        return persister_.recover_state();
    }

    void persist_state_test(uint64_t current_term, std::optional<uint64_t> voted_for) {
        auto ret = persister_.persist(current_term, voted_for);
        if (!ret) {
            LOG_ERROR("{}", ret.error());
        }
    }

    auto entries_test() const noexcept -> std::vector<LogEntry> {
        return entries_;
    }

private:
    uint64_t              first_index_;
    std::vector<LogEntry> entries_;
    Persister             persister_;
};
} // namespace foskv::raft::detail