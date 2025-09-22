#pragma once
#include "foskv/storage/storage.hpp"

namespace foskv::raft::detail {
class StateMachine {
private:
    explicit StateMachine(storage::Storage&& st);

public:
    StateMachine(StateMachine&& other) noexcept;
    auto operator=(StateMachine&& other) noexcept -> StateMachine&;

public:
    static auto create(std::string_view data_dir) -> Result<StateMachine>;

public:

private:
    storage::Storage st_;
};
} // namespace foskv::raft::detail