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

auto foskv::raft::detail::StateMachine::apply(RaftNode& node, const LogEntry &entry)
const -> RaftResult<void> {
    InternalRaftRequest request;
    if (!request.ParseFromString(entry.command())) {
        return std::unexpected{make_raft_error(RaftError::kCommandParseFailed)};
    }
    switch (request.type_case()) {
        case InternalRaftRequest::kPut: {
            auto& args = request.put();
            auto& key = args.key();
            auto& value = args.value();
            kv::PutResponse response;
            rpc::ResponseHeader header;
            if (auto status = st_.Put(key, value); !status.ok()) {
                LOG_ERROR("Failed to apply entry put {}-{} : {}", key, value, status.ToString());
                header = node.produce_internal_response_header(false, KVError::kPutFailed);
            } else {
                header = node.produce_internal_response_header(true);
            }
            response.set_allocated_header(&header);
            // return std::make_pair(request.client_id(), response.SerializeAsString());
        }
        case InternalRaftRequest::kGet: {
            auto& args = request.get();
            auto& key = args.key();
            kv::GetResponse response;
            response.mutable_kvs()->set_key(key);
            rpc::ResponseHeader header;
            if (auto status = st_.Get(key, response.mutable_kvs()->mutable_value()); !status.ok()) {
                LOG_ERROR("Failed to apply entry get {} : {}", key, status.ToString());
                if (status.IsNotFound()) {
                    header = node.produce_internal_response_header(false, KVError::kNotFound);
                } else {
                    header = node.produce_internal_response_header(true);
                }
            }
            response.set_allocated_header(&header);
            // return std::make_pair(request.client_id(), response.SerializeAsString());
        }
        case InternalRaftRequest::kDelete: {
            auto& args = request.delete_();
            auto& key = args.key();
            kv::PutResponse response;
            rpc::ResponseHeader header;
            if (auto status = st_.Delete(key); !status.ok()) {
                LOG_ERROR("Failed to apply entry delete {} : {}", key, status.ToString());
                header = node.produce_internal_response_header(false, KVError::kDeleteFailed);
            } else {
                header = node.produce_internal_response_header(true);
            }
            response.set_allocated_header(&header);
            // return std::make_pair(request.client_id(), response.SerializeAsString());
        }
        default: {
            break;
        }
    }
    LOG_ERROR("Invalid internal raft request.");
    return std::unexpected{make_raft_error(RaftError::kUnknownCommand)};
}

