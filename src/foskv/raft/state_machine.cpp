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

auto foskv::raft::detail::StateMachine::apply(RaftNode& node, const LogEntry &entry) const -> RaftResult<std::pair<uint64_t, std::string>> {
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
            auto* resp_header = response.mutable_header();
            resp_header->set_cluster_id(node.transport_.cluster_id());
            resp_header->set_member_id(node.transport_.member_id());
            resp_header->set_term(node.current_term_);
            if (auto status = st_.Put(key, value); !status.ok()) {
                auto error = status.ToString();
                LOG_ERROR("Failed to apply entry put {}-{} : {}", key, value, error);
                response.set_success(false);
                response.set_allocated_error(&error);
            }
            response.set_success(false);
            return std::make_pair(request.id(), response.SerializeAsString());
        }
        case InternalRaftRequest::kGet: {
            auto& args = request.get();
            auto& key = args.key();
            kv::GetResponse response;
            auto* resp_header = response.mutable_header();
            response.mutable_kvs()->set_key(key);
            resp_header->set_cluster_id(node.transport_.cluster_id());
            resp_header->set_member_id(node.transport_.member_id());
            resp_header->set_term(node.current_term_);
            if (auto status = st_.Get(key, response.mutable_kvs()->mutable_value()); !status.ok()) {
                auto error = status.ToString();
                LOG_ERROR("Failed to apply entry get {} : {}", key, error);
                response.set_success(false);
                response.set_allocated_error(&error);
            }
            response.set_success(true);
            return std::make_pair(request.id(), response.SerializeAsString());
        }
        case InternalRaftRequest::kDelete: {
            auto& args = request.put();
            auto& key = args.key();
            auto& value = args.value();
            kv::PutResponse response;
            auto* resp_header = response.mutable_header();
            resp_header->set_cluster_id(node.transport_.cluster_id());
            resp_header->set_member_id(node.transport_.member_id());
            resp_header->set_term(node.current_term_);
            if (auto status = st_.Delete(key); !status.ok()) {
                auto error = status.ToString();
                LOG_ERROR("Failed to apply entry put {}-{} : {}", key, value, error);
                response.set_success(false);
                response.set_allocated_error(&error);
            }
            response.set_success(true);
            return std::make_pair(request.id(), response.SerializeAsString());
        }
        default: {
            LOG_ERROR("Invalid internal raft request.");
            return std::unexpected{make_raft_error(RaftError::kUnknownCommand)};
        }
    }
}

