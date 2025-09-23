#include "foskv/raft/state_machine.hpp"

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

void foskv::raft::detail::StateMachine::produce_apply_task(ApplyTask &&task) {
    tasks_.emplace(std::move(task));
}

void foskv::raft::detail::StateMachine::apply(const Transport& transport, uint64_t& last_applied, uint64_t commit_index) {
    while (last_applied < commit_index && !tasks_.empty()) {
        auto apply_task = std::move(tasks_.front());
        tasks_.pop();
        auto session_id = apply_task.session_id_;
        auto request = std::move(apply_task.request_);
        auto session = transport.provider_.session_at(session_id);
        if (!session) {
            LOG_ERROR("Failed to apply : Session at {} not exist", session_id);
            continue;
        }

        rpc::detail::InvokeTask invoke_task;

        switch (request.cmd_case()) {
            case InternalRaftRequest::kKvPut: {
                invoke_task = apply_kv_put(request.kv_put());

                break;
            }
            case InternalRaftRequest::kKvGet: {
                invoke_task = apply_kv_get(request.kv_get());
            }
            case InternalRaftRequest::kKvDelete: {
                invoke_task = apply_kv_delete(request.kv_delete());
                break;
            }

        }
        invoke_task.request_id_ = apply_task.request_id_;
        session->tasks.push_sync(std::move(invoke_task));
        last_applied++;
    }
}

auto foskv::raft::detail::StateMachine::apply_kv_put(const kv::PutRequest &request)
const -> rpc::detail::InvokeTask {
    auto status = st_.Put(request.key(), request.value());
    rpc::detail::InvokeTask task{
            [status](std::string_view, std::span<char> resp_payload, uint64_t, uint64_t) -> kosio::async::Task<Result<std::size_t>> {
                if (!status.ok()) {
                    LOG_ERROR("Failed to put : {}", status.ToString());
                    co_return produce_kv_put_response(false, rpc::RpcError::kKVPutFailed, std::nullopt, resp_payload);
                }
                co_return produce_kv_put_response(true, rpc::RpcError::kNoError, std::nullopt, resp_payload);
    }};
    return task;
}

auto foskv::raft::detail::StateMachine::apply_kv_get(const kv::GetRequest &request)
const -> rpc::detail::InvokeTask {
    std::string value;
    auto status = st_.Get(request.key(), &value);
    auto kv = produce_kv(request.key(), std::move(value));
    rpc::detail::InvokeTask task{
            [status, kv = std::move(kv)](std::string_view, std::span<char> resp_payload, uint64_t, uint64_t) -> kosio::async::Task<Result<std::size_t>> {
                if (!status.ok()) {
                    LOG_ERROR("Failed to get : {}", status.ToString());
                    co_return produce_kv_get_response(false, rpc::RpcError::kKVGetFailed, std::nullopt, std::nullopt, resp_payload);
                }
                co_return produce_kv_get_response(true, rpc::RpcError::kNoError, std::nullopt, std::move(kv), resp_payload);
    }};
    return task;
}

auto foskv::raft::detail::StateMachine::apply_kv_delete(const kv::DeleteRequest &request)
const -> rpc::detail::InvokeTask {
    auto status = st_.Delete(request.key());
    rpc::detail::InvokeTask task{
            [status](std::string_view, std::span<char> resp_payload, uint64_t, uint64_t) -> kosio::async::Task<Result<std::size_t>> {
                if (!status.ok()) {
                    LOG_ERROR("Failed to delete : {}", status.ToString());
                    co_return produce_kv_delete_response(false, rpc::RpcError::kKVDeleteFailed, std::nullopt, resp_payload);
                }
                co_return produce_kv_delete_response(true, rpc::RpcError::kNoError, std::nullopt, resp_payload);
    }};
    return task;
}
