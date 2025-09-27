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
    std::filesystem::create_directories(path);
    auto has_st = storage::Storage::Open(options, path);
    if (!has_st) [[unlikely]] {
        return std::unexpected{has_st.error()};
    }
    return StateMachine{std::move(has_st.value())};
}

void foskv::raft::detail::StateMachine::produce_apply_task(ApplyTask task) {
    tasks_.push(std::move(task));
}

auto foskv::raft::detail::StateMachine::apply(const Transport& transport, uint64_t& last_applied, uint64_t commit_index) -> kosio::async::Task<> {
    while (last_applied < commit_index && !tasks_.empty()) {
        auto last_last_applied = last_applied++;
        auto apply_task = std::move(tasks_.front());
        tasks_.pop();
        if (apply_task.last_applied_ != last_last_applied) {
            LOG_ERROR("[{}]: Invalid last_applied.", transport.name());
            continue;
        }
        auto session_id = apply_task.session_id_;
        auto request = std::move(apply_task.request_);
        auto session = transport.provider_->session_at(session_id);

        rpc::detail::InvokeTask invoke_task;

        switch (request.cmd_case()) {
            case InternalRaftRequest::kKvPut: {
                invoke_task = apply_kv_put(request.kv_put());
                break;
            }
            case InternalRaftRequest::kKvGet: {
                invoke_task = apply_kv_get(request.kv_get());
                break;
            }
            case InternalRaftRequest::kKvDelete: {
                invoke_task = apply_kv_delete(request.kv_delete());
                break;
            }
            default: {
                LOG_ERROR("Unknown command from session {}, request_id {}", session_id, apply_task.request_id_);
                break;
            }
        }

        // Whatever with the session, the command must be applied before
        // even if we can't send response to the session
        if (session) {
            invoke_task.request_id_ = apply_task.request_id_;
            co_await session->tasks.push(std::move(invoke_task));
        }
    }
}

auto foskv::raft::detail::StateMachine::apply_kv_put(const kv::PutRequest &request)
const -> rpc::detail::InvokeTask {
    auto status = st_.Put(request.key(), request.value());
    return rpc::detail::InvokeTask(
        [status](std::string_view, std::span<char> resp_payload, uint64_t session_id, uint64_t request_id) -> kosio::async::Task<Result<std::size_t>> {
            LOG_VERBOSE("Handle request {} from session {}.", session_id, request_id);
            if (!status.ok()) {
                LOG_ERROR("Failed to put : {}", status.ToString());
                co_return produce_kv_put_response(resp_payload, false, rpc::RpcError::kKVPutFailed);
            }
            co_return produce_kv_put_response(resp_payload);
    });
}

auto foskv::raft::detail::StateMachine::apply_kv_get(const kv::GetRequest &request)
const -> rpc::detail::InvokeTask {
    std::string value;
    auto status = st_.Get(request.key(), &value);
    auto kv = produce_kv(request.key(), std::move(value));
    return rpc::detail::InvokeTask(
        [status, kv = std::move(kv)](std::string_view, std::span<char> resp_payload, uint64_t session_id, uint64_t request_id) -> kosio::async::Task<Result<std::size_t>> {
            LOG_VERBOSE("Handle request {} from session {}.", session_id, request_id);
            if (!status.ok()) {
                LOG_ERROR("Failed to get : {}", status.ToString());
                co_return produce_kv_get_response(resp_payload, false, rpc::RpcError::kKVGetFailed);
            } else {
                LOG_VERBOSE("Get kv : {}-{}", kv.key(), kv.value());
            }
            co_return produce_kv_get_response(resp_payload, true, rpc::RpcError::kNoError, kv);
    });
}

auto foskv::raft::detail::StateMachine::apply_kv_delete(const kv::DeleteRequest &request)
const -> rpc::detail::InvokeTask {
    auto status = st_.Delete(request.key());
    return rpc::detail::InvokeTask(
        [status](std::string_view, std::span<char> resp_payload, uint64_t session_id, uint64_t request_id) -> kosio::async::Task<Result<std::size_t>> {
            LOG_VERBOSE("Handle request {} from session {}.", session_id, request_id);
            if (!status.ok()) {
                LOG_ERROR("Failed to put : {}", status.ToString());
                co_return produce_kv_put_response(resp_payload, false, rpc::RpcError::kKVPutFailed);
            }
            co_return produce_kv_delete_response(resp_payload);
        });
}
