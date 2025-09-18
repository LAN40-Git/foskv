#include "foskv/raft/state_machine.hpp"

foskv::raft::detail::StateMachine::StateMachine(storage::Storage &&st)
    : st_(std::move(st)) {
}

foskv::raft::detail::StateMachine::StateMachine(StateMachine &&other) noexcept
    : st_(std::move(other.st_)) {}

auto foskv::raft::detail::StateMachine::operator=(StateMachine &&other) noexcept -> StateMachine& {
    st_ = std::move(other.st_);
    return *this;
}

auto foskv::raft::detail::StateMachine::create(const std::filesystem::path& path)
-> RaftResult<StateMachine> {
    rocksdb::Options options;
    options.create_if_missing = true;
    auto has_st = storage::Storage::Open(options, path);
    if (!has_st) [[unlikely]] {
        LOG_ERROR("{}", has_st.error());
        return std::unexpected{make_raft_error(RaftError::kPersisterCreateFailed)};
    }
    return StateMachine{std::move(has_st.value())};
}

auto foskv::raft::detail::StateMachine::apply_entries(std::span<const LogEntry> entries) -> uint64_t {
    for (auto &entry : entries) {

    }
}
