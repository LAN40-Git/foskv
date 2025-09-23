#pragma once
#include "foskv/raft/transport.hpp"
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
    void produce_apply_task(ApplyTask&& task);
    void apply(const Transport& transport, uint64_t& last_applied, uint64_t commit_index);

private:
    // kv
    auto apply_kv_put(const kv::PutRequest& request) const -> rpc::detail::InvokeTask;
    auto apply_kv_get(const kv::GetRequest& request) const -> rpc::detail::InvokeTask;
    auto apply_kv_delete(const kv::DeleteRequest& request) const -> rpc::detail::InvokeTask;

private:
    storage::Storage      st_;
    std::queue<ApplyTask> tasks_; // Only use by leader
};
} // namespace foskv::raft::detail