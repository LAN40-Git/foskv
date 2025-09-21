#include "foskv/raft/state_machine.hpp"

#include "foskv/raft/raft_node.hpp"

foskv::raft::detail::StateMachine::StateMachine(storage::Storage &&st)
    : st_(std::move(st)) {}

foskv::raft::detail::StateMachine::StateMachine(StateMachine &&other) noexcept
    : st_(std::move(other.st_)) {}

auto foskv::raft::detail::StateMachine::operator=(StateMachine &&other) noexcept -> StateMachine& {
    st_ = std::move(other.st_);
    return *this;
}

auto foskv::raft::detail::StateMachine::create(std::string_view data_dir)
-> Result<StateMachine> {
    rocksdb::Options options;
    options.create_if_missing = true;
    std::filesystem::path path(data_dir);
    path = path / USER_DATA_PATH;
    auto has_st = storage::Storage::Open(options, path);
    if (!has_st) [[unlikely]] {
        return std::unexpected{has_st.error()};
    }
    return StateMachine{std::move(has_st.value())};
}
