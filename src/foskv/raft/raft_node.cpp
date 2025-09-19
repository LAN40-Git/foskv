#include "foskv/raft/raft_node.hpp"
#include "kosio/common/util/random.hpp"

foskv::raft::RaftNode::RaftNode(Config&& config, detail::Persister&& persister, detail::StateMachine&& state_machine)
    : persister_(std::move(persister))
    , transport_(std::move(config))
    , state_machine_(std::move(state_machine)) {
    // Load Persistent
    PersistState state;
    persister_.load_state(state);
    persister_.load_entries(logs_);
    using rpc::RaftService;
    using rpc::KVService;
    // Register invokes
    transport_.provider_.register_invoke(RaftService::ServiceName, RaftService::RequestVote,
        [this](std::string_view req_payload, std::span<char> resp_payload) -> kosio::async::Task<RpcResult<void>>  {
        co_return co_await this->handle_request_vote_request(req_payload, resp_payload);
    });
    transport_.provider_.register_invoke(RaftService::ServiceName, RaftService::AppendEntries,
        [this](std::string_view req_payload, std::span<char> resp_payload) -> kosio::async::Task<RpcResult<void>> {
        co_return co_await this->handle_append_entries_request(req_payload, resp_payload);
    });
    transport_.provider_.register_invoke(RaftService::ServiceName, RaftService::InstallSnapshot,
        [this](std::string_view req_payload, std::span<char> resp_payload) -> kosio::async::Task<RpcResult<void>> {
        co_return co_await this->handle_install_snapshot_request(req_payload, resp_payload);
    });
    transport_.provider_.register_invoke(KVService::ServiceName, KVService::Put,
        [this](std::string_view req_payload, std::span<char> resp_payload) -> kosio::async::Task<RpcResult<void>> {
        InternalRaftRequest request;
        if (!request.ParseFromArray(req_payload.data(), req_payload.size())) [[unlikely]] {
            co_return std::unexpected{make_rpc_error(RpcError::kParseFailed)};
        }

        co_await mutex_.lock();
        std::lock_guard lock(mutex_, std::adopt_lock);

        if (role_.load(std::memory_order_relaxed) != kLeader) {
            // When an invalid redirect message is returned,
            // the client polls the configuration table
            rpc::Redirect redirect;
            if (leader_id_.has_value()) {
                auto it = transport_.config_.peers_.find(leader_id_.value());
                if (it != transport_.config_.peers_.end()) {
                    auto& peer = it->second;
                    redirect.set_host(peer.host());
                    redirect.set_port(peer.port());
                }
            }
            auto header = generate_internal_response_header(false, 0, redirect);
            switch (request.type_case()) {
                case InternalRaftRequest::kPut: {
                    kv::PutResponse response;
                    response.set_allocated_header(&header);
                    auto resp_payload_size = response.ByteSizeLong();
                    if (!response.SerializeToArray(resp_payload.data(), resp_payload_size)) {
                        co_return std::unexpected{make_rpc_error(RpcError::kSerializeFailed)};
                    }
                    resp_payload = resp_payload.subspan(0, resp_payload_size);
                    co_return RpcResult<void>{};
                }
                case InternalRaftRequest::kGet: {
                    kv::GetResponse response;
                    response.set_allocated_header(&header);
                    auto resp_payload_size = response.ByteSizeLong();
                    if (!response.SerializeToArray(resp_payload.data(), resp_payload_size)) {
                        co_return std::unexpected{make_rpc_error(RpcError::kSerializeFailed)};
                    }
                    resp_payload = resp_payload.subspan(0, resp_payload_size);
                    co_return RpcResult<void>{};
                }
                case InternalRaftRequest::kDelete: {
                    kv::DeleteResponse response;
                    response.set_allocated_header(&header);
                    auto resp_payload_size = response.ByteSizeLong();
                    if (!response.SerializeToArray(resp_payload.data(), resp_payload_size)) {
                        co_return std::unexpected{make_rpc_error(RpcError::kSerializeFailed)};
                    }
                    resp_payload = resp_payload.subspan(0, resp_payload_size);
                    co_return RpcResult<void>{};
                }
                default: {
                    co_return std::unexpected{make_rpc_error(RpcError::kRpcServiceNotExists)};
                }
            }
        }

        // Packaged as a log entry
        LogEntry entry;
        entry.set_index(logs_.size());
        entry.set_term(logs_.back().term());
        entry.set_command(std::string{req_payload});
        auto entry_payload_size = entry.ByteSizeLong();
        auto index = std::to_string(entry.index());
        if (!entry.SerializeToArray(buffer_.data(), entry_payload_size)) [[unlikely]] {
            co_return std::unexpected{make_rpc_error(RpcError::kSerializeFailed)};
        }
        auto has_been_persistent = persister_.persist_entry(
            index,
            {buffer_.data(), entry_payload_size});
        if (!has_been_persistent) {
            co_return std::unexpected{make_rpc_error(RpcError::kSerializeFailed)};
        }
        // Copy the entry for sync
        LogEntry copy_entry;
        copy_entry.CopyFrom(entry);
        // Persister and Append log entry
        logs_.push_back(std::move(entry));

        // Synchronize log entry
        auto append_request = produce_append_entries_request();
        auto new_entry = append_request.add_entries();
        new_entry->Swap(&copy_entry);
        kosio::spawn(transport_.broadcast_append_entries_request(std::move(append_request), [this](std::string_view resp_payload) -> kosio::async::Task<void> {
            AppendEntriesResponse response;
            if (!response.ParseFromArray(resp_payload.data(), resp_payload.size())) [[unlikely]] {
                LOG_ERROR("Failed to parse request vote response");
                co_return;
            }

            auto resp_term = response.header().term();
            auto success = response.success();

            if (resp_term < current_term_.load(std::memory_order_relaxed)) {
                co_return;
            }

            co_await mutex_.lock();
            std::lock_guard lock(mutex_, std::adopt_lock);

            // Check again
            auto current_term = current_term_.load(std::memory_order_relaxed);
            if (resp_term < current_term) {
                co_return;
            }

            if (resp_term > current_term) {
                increase_term_to(resp_term);
                role_.store(kFollower, std::memory_order_relaxed);
                last_reset_time_.store(kosio::util::current_ms(), std::memory_order_relaxed);
                co_return;
            }

            if (role_.load(std::memory_order_relaxed) != kLeader) {
                co_return;
            }
        }));
    });
}

foskv::raft::RaftNode::RaftNode(RaftNode &&other) noexcept
    : is_shutdown_(other.is_shutdown_.load(std::memory_order_relaxed))
      , persister_(std::move(other.persister_))
      , transport_(std::move(other.transport_))
      , last_reset_time_(other.last_reset_time_.load(std::memory_order_relaxed))
      , state_machine_(std::move(other.state_machine_))
      , role_(other.role_.load(std::memory_order_relaxed))
      , current_term_(other.current_term_.load(std::memory_order_relaxed))
      , voted_for_(other.voted_for_)
      , logs_(std::move(other.logs_))
      , commit_index_(other.commit_index_)
      , last_applied_(other.last_applied_)
      , leader_id_(other.leader_id_)
      , next_index_(std::move(other.next_index_))
      , match_index_(std::move(other.match_index_)) {
}

auto foskv::raft::RaftNode::operator=(RaftNode &&other) noexcept -> RaftNode & {
    is_shutdown_ = other.is_shutdown_.load(std::memory_order_relaxed);
    persister_ = std::move(other.persister_);
    transport_ = std::move(other.transport_);
    last_reset_time_ = other.last_reset_time_.load(std::memory_order_relaxed);
    state_machine_ = std::move(other.state_machine_);
    role_ = other.role_.load(std::memory_order_relaxed);
    current_term_ = other.current_term_.load(std::memory_order_relaxed);
    voted_for_ = other.voted_for_;
    logs_ = std::move(other.logs_);
    commit_index_ = other.commit_index_;
    last_applied_ = other.last_applied_;
    leader_id_ = other.leader_id_;
    next_index_ = std::move(other.next_index_);
    match_index_ = std::move(other.match_index_);
    return *this;
}

auto foskv::raft::RaftNode::create(
    const std::filesystem::path& config_path,
    const std::filesystem::path& data_dir) -> kosio::async::Task<RaftResult<RaftNode>> {
    // Load config
    auto has_config = co_await Config::load(config_path);
    if (!has_config) [[unlikely]] {
        co_return std::unexpected{has_config.error()};
    }

    // Create persister and load persistent
    auto has_persister = detail::Persister::create(data_dir / detail::PERSISTENT_PATH);
    if (!has_persister) [[unlikely]] {
        co_return std::unexpected{has_persister.error()};
    }

    // Create state machine
    auto has_state_machine = detail::StateMachine::create(data_dir / detail::USER_DATA_PATH);
    if (!has_state_machine) {
        throw std::runtime_error("Failed to create state machine");
    }
    co_return RaftNode{std::move(has_config.value()), std::move(has_persister.value()), std::move(has_state_machine.value())};
}

auto foskv::raft::RaftNode::run() -> kosio::async::Task<> {
    kosio::spawn(start_election_timeout());
    kosio::spawn(start_heartbeat_timeout());
    co_await transport_.run();
}

auto foskv::raft::RaftNode::start_election_timeout() -> kosio::async::Task<> {
    kosio::util::FastRand fast_rand;
    while (!is_shutdown_.load(std::memory_order_relaxed)) {
        // 150-300ms election timeout
        auto election_timeout = fast_rand.rand_range(150, 300);
        co_await kosio::time::sleep(election_timeout);

        // continue if `!(current_ms - last_reset_time_ >= election_timeout)`
        if (kosio::util::current_ms() < last_reset_time_.load(std::memory_order_relaxed) + election_timeout) {
            continue;
        }

        if (role_.load(std::memory_order_relaxed) == kLeader) {
            continue;
        }

        co_await mutex_.lock();
        std::lock_guard lock(mutex_, std::adopt_lock);

        // Check again
        if (role_.load(std::memory_order_relaxed) == kLeader) {
            continue;
        }

        auto current_term = current_term_.load(std::memory_order_relaxed);
        increase_term_to(current_term++);

        RequestVoteRequest request;
        request.set_candidate_id(transport_.member_id());
        request.set_term(current_term);
        auto last_log_index = logs_.size();
        auto last_log_term = (last_log_index == 0) ? 0 : logs_[last_log_index].term();
        request.set_last_log_index(last_log_index);
        request.set_last_log_term(last_log_term);
        kosio::spawn(transport_.broadcast_request_vote_request(std::move(request),
        [this](std::string_view resp_payload) -> kosio::async::Task<> {
                RequestVoteResponse response;
                if (!response.ParseFromArray(resp_payload.data(), resp_payload.size())) [[unlikely]] {
                    LOG_ERROR("Failed to parse request vote response");
                    co_return;
                }

                auto resp_term = response.header().term();
                auto vote_granted = response.vote_granted();
                // Votes for local raftnode at current term
                static std::size_t votes{0};

                if (resp_term < current_term_.load(std::memory_order_relaxed)) {
                    co_return;
                }

                co_await mutex_.lock();
                std::lock_guard lock(mutex_, std::adopt_lock);

                // Check again
                auto current_term = current_term_.load(std::memory_order_relaxed);
                if (resp_term < current_term) {
                    co_return;
                }

                if (resp_term > current_term) {
                    votes = 0;
                    increase_term_to(resp_term);
                    role_.store(kFollower, std::memory_order_relaxed);
                    last_reset_time_.store(kosio::util::current_ms(), std::memory_order_relaxed);
                    co_return;
                }

                if (role_.load(std::memory_order_relaxed) == kLeader) {
                    co_return;
                }

                if (vote_granted) {
                    votes += 1;
                    if (votes > transport_.peer_count() / 2) {
                        become_leader();
                    }
                }
        }));
    }
}

auto foskv::raft::RaftNode::start_heartbeat_timeout() -> kosio::async::Task<> {
    while (!is_shutdown_.load(std::memory_order_relaxed)) {
        // 50ms heartbeat timeout
        co_await kosio::time::sleep(50);

        if (role_.load(std::memory_order_relaxed) != kLeader) {
            continue;
        }

        co_await mutex_.lock();
        std::lock_guard lock(mutex_, std::adopt_lock);

        // Check again
        if (role_.load(std::memory_order_relaxed) != kLeader) {
            continue;
        }

        AppendEntriesRequest request;
        request.set_leader_id(transport_.member_id());
        request.set_term(current_term_.load(std::memory_order_relaxed));
        auto prev_log_index = logs_.size();
        auto prev_log_term = (prev_log_index == 0) ? 0 : logs_[prev_log_index].term();
        request.set_prev_log_index(prev_log_index);
        request.set_prev_log_term(prev_log_term);
        request.set_leader_commit(commit_index_);
        kosio::spawn(transport_.broadcast_append_entries_request(std::move(request),
            [this](std::string_view resp_payload) -> kosio::async::Task<> {
                AppendEntriesResponse response;
                if (!response.ParseFromArray(resp_payload.data(), resp_payload.size())) [[unlikely]] {
                    LOG_ERROR("Failed to parse request vote response");
                    co_return;
                }

                auto resp_term = response.header().term();
                [[maybe_unused]] auto success = response.success();

                if (resp_term < current_term_.load(std::memory_order_relaxed)) {
                    co_return;
                }

                co_await mutex_.lock();
                std::lock_guard lock(mutex_, std::adopt_lock);

                // Check again
                auto current_term = current_term_.load(std::memory_order_relaxed);
                if (resp_term < current_term) {
                    co_return;
                }

                if (resp_term > current_term) {
                    increase_term_to(resp_term);
                    role_.store(kFollower, std::memory_order_relaxed);
                    last_reset_time_.store(kosio::util::current_ms(), std::memory_order_relaxed);
                    co_return;
                }

                if (role_.load(std::memory_order_relaxed) != kLeader) {
                    co_return;
                }
        }));
    }
}

void foskv::raft::RaftNode::increase_term_to(uint64_t term) {
    // Holding mutex_ here, so use relaxed memory
    current_term_.store(term, std::memory_order_relaxed);
    voted_for_ = std::nullopt;
}

void foskv::raft::RaftNode::become_leader() {
    // Holding mutex_ here, so use relaxed memory
    role_.store(kLeader, std::memory_order_relaxed);
}

auto foskv::raft::RaftNode::handle_request_vote_request(std::string_view req_payload, std::span<char> resp_payload)
-> kosio::async::Task<RpcResult<void>> {
    RequestVoteRequest request;
    if (!request.ParseFromArray(req_payload.data(), req_payload.size())) [[unlikely]] {
        co_return std::unexpected{make_rpc_error(RpcError::kParseFailed)};
    }

    std::size_t resp_payload_size;
    auto candidate_id = request.candidate_id();
    auto req_term = request.term();
    auto last_log_index = request.last_log_index();
    auto last_log_term = request.last_log_term();

    if (req_term < current_term_.load(std::memory_order_relaxed)) {
        RequestVoteResponse response;
        auto header = generate_response_header();
        response.set_allocated_header(&header);
        response.set_vote_granted(false);
        resp_payload_size = response.ByteSizeLong();
        if (!response.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
            co_return std::unexpected{make_rpc_error(RpcError::kSerializeFailed)};
        }
        resp_payload = resp_payload.subspan(0, resp_payload_size);
        co_return RpcResult<void>{};
    }

    co_await mutex_.lock();
    std::lock_guard lock(mutex_, std::adopt_lock);

    // Check again
    auto current_term = current_term_.load(std::memory_order_relaxed);
    if (req_term < current_term) {
        RequestVoteResponse response;
        auto header = generate_response_header();
        response.set_allocated_header(&header);
        response.set_vote_granted(false);
        resp_payload_size = response.ByteSizeLong();
        if (!response.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
            co_return std::unexpected{make_rpc_error(RpcError::kSerializeFailed)};
        }
        resp_payload = resp_payload.subspan(0, resp_payload_size);
        co_return RpcResult<void>{};
    }

    if (req_term > current_term) {
        current_term = req_term;
        increase_term_to(req_term);
        role_.store(kFollower, std::memory_order_relaxed);
        last_reset_time_.store(kosio::util::current_ms(), std::memory_order_relaxed);
    }

    // Can vote for the candidate
    bool can_vote = (!voted_for_ || voted_for_.value() == candidate_id);
    // Whether the logs of candidate are up to date
    bool up_to_date_log = false;

    if (last_log_index > logs_.size() ||
        (last_log_index == logs_.size() && last_log_term == logs_[last_log_index].term())) {
        up_to_date_log = true;
    }

    RequestVoteResponse response;
    auto header = generate_response_header();
    response.set_allocated_header(&header);
    response.set_vote_granted(can_vote && up_to_date_log);
    resp_payload_size = response.ByteSizeLong();
    if (!response.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
        co_return std::unexpected{make_rpc_error(RpcError::kSerializeFailed)};
    }
    resp_payload = resp_payload.subspan(0, resp_payload_size);
    co_return RpcResult<void>{};
}

auto foskv::raft::RaftNode::handle_append_entries_request(std::string_view req_payload, std::span<char> resp_payload)
-> kosio::async::Task<RpcResult<void>> {
    AppendEntriesRequest request;
    if (!request.ParseFromArray(req_payload.data(), req_payload.size())) [[unlikely]] {
        co_return std::unexpected{make_rpc_error(RpcError::kParseFailed)};
    }

    std::size_t resp_payload_size;
    auto leader_id = request.leader_id();
    auto req_term = request.term();
    auto prev_log_index = request.prev_log_index();
    auto prev_log_term = request.prev_log_term();
    auto entries = request.entries();
    auto leader_commit = request.leader_commit();

    if (req_term < current_term_.load(std::memory_order_relaxed)) {
        AppendEntriesResponse response;
        auto header = generate_response_header();
        response.set_allocated_header(&header);
        response.set_success(false);
        resp_payload_size = response.ByteSizeLong();
        if (!response.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
            co_return std::unexpected{make_rpc_error(RpcError::kSerializeFailed)};
        }
        resp_payload = resp_payload.subspan(0, resp_payload_size);
        co_return RpcResult<void>{};
    }

    co_await mutex_.lock();
    std::lock_guard lock(mutex_, std::adopt_lock);

    // Check again
    auto current_term = current_term_.load(std::memory_order_relaxed);
    if (req_term < current_term) {
        AppendEntriesResponse response;
        auto header = generate_response_header();
        response.set_allocated_header(&header);
        response.set_success(false);
        resp_payload_size = response.ByteSizeLong();
        if (!response.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
            co_return std::unexpected{make_rpc_error(RpcError::kSerializeFailed)};
        }
        resp_payload = resp_payload.subspan(0, resp_payload_size);
        co_return RpcResult<void>{};
    }

    if (req_term > current_term) {
        current_term = req_term;
        increase_term_to(req_term);
        role_.store(kFollower, std::memory_order_relaxed);
        last_reset_time_.store(kosio::util::current_ms(), std::memory_order_relaxed);
    }

    leader_id_ = leader_id;

    auto last_log_index = logs_.size();

    // Heartbeat
    if (entries.empty()) {
        last_reset_time_.store(kosio::util::current_ms(), std::memory_order_relaxed);
        commit_index_ = std::min(last_log_index, leader_commit);
        AppendEntriesResponse response;
        auto header = generate_response_header();
        response.set_allocated_header(&header);
        response.set_success(true);
        resp_payload_size = response.ByteSizeLong();
        if (!response.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
            co_return std::unexpected{make_rpc_error(RpcError::kSerializeFailed)};
        }
        resp_payload = resp_payload.subspan(0, resp_payload_size);
        co_return RpcResult<void>{};
    }

    if (prev_log_index > last_log_index) {
        // Our log is too old, return false
        AppendEntriesResponse response;
        auto header = generate_response_header();
        response.set_allocated_header(&header);
        response.set_success(false);
        resp_payload_size = response.ByteSizeLong();
        if (!response.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
            co_return std::unexpected{make_rpc_error(RpcError::kSerializeFailed)};
        }
        resp_payload = resp_payload.subspan(0, resp_payload_size);
        co_return RpcResult<void>{};
    }

    if (logs_[prev_log_index].term() != prev_log_term) {
        logs_.erase(logs_.begin() + prev_log_index, logs_.end());
    }

    for (auto& entry : entries) {
        logs_.push_back(std::move(entry));
    }
    commit_index_ = std::min(last_log_index, leader_commit);

    AppendEntriesResponse response;
    auto header = generate_response_header();
    response.set_allocated_header(&header);
    response.set_success(true);
    resp_payload_size = response.ByteSizeLong();
    if (!response.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
        co_return std::unexpected{make_rpc_error(RpcError::kSerializeFailed)};
    }
    resp_payload = resp_payload.subspan(0, resp_payload_size);
    co_return RpcResult<void>{};
}

auto foskv::raft::RaftNode::handle_install_snapshot_request(std::string_view req_payload, std::span<char> resp_payload)
-> kosio::async::Task<RpcResult<void>> {
    InstallSnapshotRequest request;
    if (!request.ParseFromArray(req_payload.data(), req_payload.size())) [[unlikely]] {
        co_return std::unexpected{make_rpc_error(RpcError::kParseFailed)};
    }

}

auto foskv::raft::RaftNode::handle_internal_raft_request(std::string_view req_payload, std::span<char> resp_payload)
-> kosio::async::Task<RpcResult<void>> {

}

auto foskv::raft::RaftNode::generate_response_header()
const noexcept -> ResponseHeader {
    ResponseHeader header;
    header.set_cluster_id(transport_.cluster_id());
    header.set_member_id(transport_.member_id());
    header.set_term(current_term_.load(std::memory_order_relaxed));
    return header;
}

auto foskv::raft::RaftNode::generate_internal_response_header(bool success, int error_code, std::optional<rpc::Redirect> redirect)
const noexcept -> rpc::ResponseHeader {
    rpc::ResponseHeader header;
    header.set_cluster_id(transport_.cluster_id());
    header.set_member_id(transport_.member_id());
    header.set_term(current_term_.load(std::memory_order_relaxed));
    header.set_success(success);
    header.set_error_code(error_code);
    if (redirect.has_value()) {
        header.set_allocated_redirect(&redirect.value());
    }
    return header;
}

auto foskv::raft::RaftNode::produce_append_entries_request()
const noexcept -> AppendEntriesRequest {
    AppendEntriesRequest request;
    request.set_term(current_term_.load(std::memory_order_relaxed));
    request.set_leader_id(transport_.member_id());
    request.set_leader_commit(commit_index_);
    request.set_prev_log_index(logs_.size() - 1);
    request.set_prev_log_term(logs_[logs_.size() - 1].term());
    return request;
}
