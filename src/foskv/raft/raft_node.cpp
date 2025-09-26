#include "foskv/raft/raft_node.hpp"
#include "kosio/common/util/random.hpp"

foskv::raft::RaftNode::RaftNode(
    RaftConfig&& config,
    detail::StateMachine&& state_machine,
    PersistState&& state,
    detail::RaftLog&& logs)
    : transport_(std::move(config))
    , state_machine_(std::move(state_machine))
    , logs_(std::move(logs)) {
    LOG_VERBOSE("[{}] : Successfully recover state : current_term : {}, voted_for : {}", transport_.name(), state.current_term(), state.voted_for());
    current_term_ = state.current_term();
    if (state.has_voted_for()) {
        voted_for_ = state.voted_for();
    }
    // Register invokes
    using rpc::ServiceType;
    using rpc::MethodType;
    // RequestVote
    transport_.provider_.register_invoke(ServiceType::kRaft, MethodType::kRaftRequestVote,
        [this](std::string_view req_payload, std::span<char> resp_payload, uint64_t, uint64_t) -> kosio::async::Task<Result<std::size_t>>  {
        co_return co_await this->handle_request_vote_request(req_payload, resp_payload);
    });
    // AppendEntries
    transport_.provider_.register_invoke(ServiceType::kRaft, MethodType::kRaftAppendEntries,
        [this](std::string_view req_payload, std::span<char> resp_payload, uint64_t, uint64_t) -> kosio::async::Task<Result<std::size_t>> {
        co_return co_await this->handle_append_entries_request(req_payload, resp_payload);
    });
    // InstallSnapshot
    transport_.provider_.register_invoke(ServiceType::kRaft, MethodType::kRaftInstallSnapshot,
        [this](std::string_view req_payload, std::span<char> resp_payload, uint64_t, uint64_t) -> kosio::async::Task<Result<std::size_t>> {
        co_return co_await this->handle_install_snapshot_request(req_payload, resp_payload);
    });
    // KVPut
    transport_.provider_.register_invoke(ServiceType::kKv, MethodType::kKvPut,
        [this](std::string_view req_payload, std::span<char> resp_payload, uint64_t session_id, uint64_t request_id) -> kosio::async::Task<Result<std::size_t>> {
        co_return co_await this->handle_kv_put_request(req_payload, resp_payload, session_id, request_id);
    });
    // KVGet
    transport_.provider_.register_invoke(ServiceType::kKv, MethodType::kKvGet,
        [this](std::string_view req_payload, std::span<char> resp_payload, uint64_t session_id, uint64_t request_id) -> kosio::async::Task<Result<std::size_t>> {
        co_return co_await this->handle_kv_get_request(req_payload, resp_payload, session_id, request_id);
    });
    // KVDelete
    transport_.provider_.register_invoke(ServiceType::kKv, MethodType::kKvDelete,
        [this](std::string_view req_payload, std::span<char> resp_payload, uint64_t session_id, uint64_t request_id) -> kosio::async::Task<Result<std::size_t>> {
        co_return co_await this->handle_kv_delete_request(req_payload, resp_payload, session_id, request_id);
    });
}

auto foskv::raft::RaftNode::create(std::string_view config_path, std::string_view data_dir)
-> kosio::async::Task<Result<std::unique_ptr<RaftNode>>> {
    // Load config
    auto has_config = co_await RaftConfig::load(config_path);
    if (!has_config) {
        co_return std::unexpected{has_config.error()};
    }

    // Create state machine
    auto has_state_machine = detail::StateMachine::create(data_dir);
    if (!has_state_machine) {
        co_return std::unexpected{has_state_machine.error()};
    }

    // Create raft log, recover the entries and state
    auto has_logs = detail::RaftLog::create(data_dir);
    if (!has_logs) {
        co_return std::unexpected{has_logs.error()};
    }
    // recover state
    auto has_state = has_logs.value().persister_.recover_state();
    if (!has_state) {
        co_return std::unexpected{has_state.error()};
    }

    // Return a valid raftnode
    co_return std::make_unique<RaftNode>(
        std::move(has_config.value()),        // Config
        std::move(has_state_machine.value()), // StateMachine
        std::move(has_state.value()),         // PersistState
        std::move(has_logs.value())           // Logs
    );
}

auto foskv::raft::RaftNode::run() -> kosio::async::Task<Result<void>> {
    kosio::spawn(start_election_timeout());
    kosio::spawn(start_heartbeat_timeout());
    co_return co_await transport_.run();
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

        if (role_.load(std::memory_order_acquire) == kLeader) {
            continue;
        }

        co_await mutex_.lock();
        std::lock_guard lock(mutex_, std::adopt_lock);

        // Check again
        if (role_.load(std::memory_order_relaxed) == kLeader) {
            continue;
        }

        current_term_.fetch_add(1, std::memory_order_relaxed);

        // Broadcast request vote request
        co_await transport_.broadcast_request_vote_request(produce_request_vote_request(),
        [this](std::string_view resp_payload) -> kosio::async::Task<> {
                co_await this->handle_request_vote_response(resp_payload);
        });
    }
}

auto foskv::raft::RaftNode::start_heartbeat_timeout() -> kosio::async::Task<> {
    while (!is_shutdown_.load(std::memory_order_relaxed)) {
        // 50ms heartbeat timeout
        co_await kosio::time::sleep(50);

        if (role_.load(std::memory_order_acquire) != kLeader) {
            continue;
        }

        co_await mutex_.lock();
        std::lock_guard lock(mutex_, std::adopt_lock);

        // Check again
        if (role_.load(std::memory_order_relaxed) != kLeader) {
            continue;
        }

        // Broadcast append_entries_request (heartbeat)
        co_await transport_.broadcast_append_entries_request(produce_append_entries_request(),
            [this](std::string_view resp_payload) -> kosio::async::Task<> {
                co_await this->handle_heartbeat_response(resp_payload);
        });
    }
}

void foskv::raft::RaftNode::increase_term_to(uint64_t term) {
    // Holding mutex_ here, so use relaxed memory
    current_term_.store(term, std::memory_order_release);
    voted_for_ = std::nullopt;
}

void foskv::raft::RaftNode::become_leader() {
    // Holding mutex_ here, so use relaxed memory
    role_.store(kLeader, std::memory_order_release);
    LOG_VERBOSE("[{}]: Become leader, current_term : {}.", transport_.name(), current_term_.load(std::memory_order_relaxed));
}

auto foskv::raft::RaftNode::handle_request_vote_response(std::string_view resp_payload) -> kosio::async::Task<> {
    RequestVoteResponse response;
    if (!response.ParseFromArray(resp_payload.data(), resp_payload.size())) [[unlikely]] {
        LOG_ERROR("Failed to parse request vote response");
        co_return;
    }

    auto resp_term = response.header().term();
    auto vote_granted = response.vote_granted();

    if (resp_term < current_term_.load(std::memory_order_acquire)) {
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
        votes_ = 0;
        increase_term_to(resp_term);
        role_.store(kFollower, std::memory_order_release);
        last_reset_time_.store(kosio::util::current_ms(), std::memory_order_relaxed);
        co_return;
    }

    if (role_.load(std::memory_order_acquire) == kLeader) {
        co_return;
    }

    if (vote_granted) {
        votes_ += 1;
        if (votes_ > transport_.peer_count() / 2) {
            become_leader();
        }
    }
}

auto foskv::raft::RaftNode::handle_heartbeat_response(std::string_view resp_payload) -> kosio::async::Task<> {
    AppendEntriesResponse response;
    if (!response.ParseFromArray(resp_payload.data(), static_cast<int>(resp_payload.size()))) [[unlikely]] {
        LOG_ERROR("Failed to parse request vote response");
        co_return;
    }

    auto member_id = response.header().member_id();
    auto resp_term = response.header().term();
    if (resp_term < current_term_.load(std::memory_order_acquire) ||
        role_.load(std::memory_order_acquire) != kLeader) {
        co_return;
        }

    co_await mutex_.lock();
    std::lock_guard lock(mutex_, std::adopt_lock);

    // Check again
    auto current_term = current_term_.load(std::memory_order_relaxed);
    if (resp_term < current_term ||
        role_.load(std::memory_order_relaxed) != kLeader) {
        co_return;
    }

    if (resp_term > current_term) {
        current_term = resp_term;
        increase_term_to(resp_term);
        role_.store(kFollower, std::memory_order_release);
        last_reset_time_.store(kosio::util::current_ms(), std::memory_order_relaxed);
        auto ret = logs_.persister_.persist(current_term, voted_for_);
        if (!ret) [[unlikely]] {
            LOG_FATAL("[{}]: Failed to persist state, current_term : {}, voted_for : {}", transport_.name(), current_term, voted_for_.value_or(0));
        }
    }

    auto it = transport_.config_.peers_.find(member_id);
    if (it == transport_.config_.peers_.end()) {
        co_return;
    }
    LOG_VERBOSE("[{}]: Heartbeat from {}, current_term : {}", transport_.name(), it->second.name(), current_term);
}

auto foskv::raft::RaftNode::handle_append_entries_response(std::string_view resp_payload, uint64_t match_index,
                                                           std::size_t apply_size) -> kosio::async::Task<> {
    AppendEntriesResponse response;
    if (!response.ParseFromArray(resp_payload.data(), resp_payload.size())) [[unlikely]] {
        LOG_ERROR("Failed to parse request vote response");
        co_return;
    }

    auto member_id = response.header().member_id();
    auto resp_term = response.header().term();
    auto success = response.success();

    if (resp_term < current_term_.load(std::memory_order_acquire) ||
        role_.load(std::memory_order_acquire) != kLeader) {
        co_return;
    }

    co_await mutex_.lock();
    std::lock_guard lock(mutex_, std::adopt_lock);

    // Check again
    auto current_term = current_term_.load(std::memory_order_relaxed);
    if (resp_term < current_term ||
        role_.load(std::memory_order_relaxed) != kLeader) {
        co_return;
    }

    if (resp_term > current_term) {
        current_term = resp_term;
        increase_term_to(resp_term);
        role_.store(kFollower, std::memory_order_release);
        last_reset_time_.store(kosio::util::current_ms(), std::memory_order_relaxed);
        auto ret = logs_.persister_.persist(current_term, voted_for_);
        if (!ret) [[unlikely]] {
            LOG_FATAL("[{}]: Failed to persist state, current_term : {}, voted_for : {}", transport_.name(), current_term, voted_for_.value_or(0));
        }
        co_return;
    }

    if (!success) {
        next_index_[member_id]--;
        co_return;
    }

    // Update commit_index、match_index、next_index
    match_index_[member_id] = match_index + apply_size;
    next_index_[member_id] = match_index_[member_id] + 1;
    std::vector<uint64_t> idxs;
    idxs.reserve(match_index_.size());
    for (auto idx: match_index_ | std::views::values) {
        idxs.push_back(idx);
    }
    std::ranges::sort(idxs);
    commit_index_ = idxs[idxs.size()/2];

    // Apply commands
    co_await state_machine_.apply(transport_, last_applied_, commit_index_);
}

auto foskv::raft::RaftNode::handle_install_snapshot_response(std::string_view resp_payload) -> kosio::async::Task<> {

}

auto foskv::raft::RaftNode::handle_request_vote_request(
    std::string_view req_payload, std::span<char> resp_payload, uint64_t, uint64_t)
-> kosio::async::Task<Result<std::size_t>> {
    RequestVoteRequest request;
    if (!request.ParseFromArray(req_payload.data(), req_payload.size())) [[unlikely]] {
        co_return std::unexpected{make_error(Error::kRequestVoteRequestRequestParseFailed)};
    }

    auto candidate_id = request.candidate_id();
    auto req_term = request.term();
    auto last_log_index = request.last_log_index();
    auto last_log_term = request.last_log_term();

    co_await mutex_.lock();
    std::lock_guard lock(mutex_, std::adopt_lock);

    auto current_term = current_term_.load(std::memory_order_relaxed);
    if (req_term < current_term) {
        co_return produce_request_vote_response(false, resp_payload);
    }

    if (req_term > current_term) {
        current_term = req_term;
        increase_term_to(current_term);
        role_.store(kFollower, std::memory_order_relaxed);
        last_reset_time_.store(kosio::util::current_ms(), std::memory_order_relaxed);
        auto ret = logs_.persister_.persist(current_term, voted_for_);
        if (!ret) [[unlikely]] {
            LOG_FATAL("[{}]: Failed to persist state, current_term : {}, voted_for : {}", transport_.name(), current_term, voted_for_.value_or(0));
        }
    }

    // Can vote for the candidate
    bool can_vote = (!voted_for_ || voted_for_.value() == candidate_id);
    // Whether the logs of candidate are up to date
    bool up_to_date_log = false;

    if (last_log_index > logs_.last_log_index() ||
        (last_log_index == logs_.last_log_index() &&
            last_log_term == logs_.last_log_term())) {
        up_to_date_log = true;
    }

    co_return produce_request_vote_response(can_vote && up_to_date_log, resp_payload);
}

auto foskv::raft::RaftNode::handle_append_entries_request(
    std::string_view req_payload, std::span<char> resp_payload, uint64_t, uint64_t)
-> kosio::async::Task<Result<std::size_t>> {
    AppendEntriesRequest request;
    if (!request.ParseFromArray(req_payload.data(), req_payload.size())) [[unlikely]] {
        co_return std::unexpected{make_error(Error::kAppendEntriesRequestParseFailed)};
    }

    auto leader_id = request.leader_id();
    auto req_term = request.term();
    auto prev_log_index = request.prev_log_index();
    auto prev_log_term = request.prev_log_term();
    auto entries = request.entries();
    auto leader_commit = request.leader_commit();

    co_await mutex_.lock();
    std::lock_guard lock(mutex_, std::adopt_lock);

    auto current_term = current_term_.load(std::memory_order_relaxed);
    if (req_term < current_term) {
        co_return produce_append_entries_response(false, resp_payload);
    }

    if (req_term > current_term) {
        current_term = req_term;
        increase_term_to(current_term);
        role_.store(kFollower, std::memory_order_relaxed);
        last_reset_time_.store(kosio::util::current_ms(), std::memory_order_relaxed);
        auto ret = logs_.persister_.persist(current_term, voted_for_);
        if (!ret) [[unlikely]] {
            LOG_FATAL("[{}]: Failed to persist state, current_term : {}, voted_for : {}", transport_.name(), current_term, voted_for_.value_or(0));
        }
    }

    leader_id_ = leader_id;

    // Heartbeat
    if (entries.empty()) {
        last_reset_time_.store(kosio::util::current_ms(), std::memory_order_relaxed);
        if (leader_commit > commit_index_) {
            commit_index_ = std::min(logs_.last_log_index(), leader_commit);
        }
        co_return produce_append_entries_response(true, resp_payload);
    }

    if (prev_log_index > logs_.last_log_index()) {
        // Our log is too old, return false
        co_return produce_append_entries_response(false, resp_payload);
    }

    if (logs_.term_at(prev_log_index) != prev_log_term) {
        logs_.truncate_entries(prev_log_index);
    }

    {
        std::vector<LogEntry> move_entries;
        move_entries.reserve(entries.size());
        move_entries.insert(
            move_entries.begin(),
            std::make_move_iterator(entries.begin()),
            std::make_move_iterator(entries.end()));
        auto has_append = logs_.append_entries(std::move(move_entries));
        if (!has_append) {
            LOG_ERROR("{}", has_append.error());
            co_return produce_append_entries_response(false, resp_payload);
        }
    }

    if (leader_commit > commit_index_) {
        commit_index_ = std::min(logs_.last_log_index(), leader_commit);
    }

    co_return produce_append_entries_response(true, resp_payload);
}

auto foskv::raft::RaftNode::handle_install_snapshot_request(
    std::string_view req_payload, std::span<char> resp_payload, uint64_t, uint64_t)
-> kosio::async::Task<Result<std::size_t>> {
    InstallSnapshotRequest request;
    if (!request.ParseFromArray(req_payload.data(), req_payload.size())) {
        co_return std::unexpected{make_error(Error::kInstallSnapshotRequestParseFailed)};
    }

    auto req_term = request.term();
    auto leader_id = request.leader_id();
    auto last_included_index = request.last_included_index();
    auto last_included_term = request.last_included_term();
    auto offset = request.offset();
    auto data = request.data();
    bool done = request.done();

    co_await mutex_.lock();
    std::lock_guard lock(mutex_, std::adopt_lock);

    auto current_term = current_term_.load(std::memory_order_relaxed);
    if (req_term < current_term) {
        co_return produce_install_snapshot_response(resp_payload);
    }

    if (offset == 0) {
        // TODO: Create new snapshot file
    }

    // TODO: Write data into snapshot file at given offset

}

auto foskv::raft::RaftNode::handle_kv_put_request(std::string_view req_payload, std::span<char> resp_payload,
    uint64_t session_id, uint64_t request_id) -> kosio::async::Task<Result<std::size_t>> {
    kv::PutRequest request;
    if (!request.ParseFromArray(req_payload.data(), req_payload.size())) {
        co_return std::unexpected{make_error(Error::kKVPutRequestParseFailed)};
    }

    co_await mutex_.lock();
    std::lock_guard lock(mutex_, std::adopt_lock);

    // Return redirect response
    if (role_.load(std::memory_order_relaxed) != kLeader) {
        std::optional<rpc::Redirect> redirect{std::nullopt};
        // Try to get redirect info
        if (leader_id_) {
            auto leader = transport_.config_.peers_.find(leader_id_.value());
            if (leader != transport_.config_.peers_.end()) {
                auto host = leader->second.host();
                auto port = leader->second.port();
                redirect = detail::produce_redirect(std::move(host), port);
            }
        }
        // Tell the client that it need to redirect to the leader
        // If 'redirect' = 'std::nullopt', it means that
        // the client needs to determine the redirect address itself
        co_return detail::produce_kv_put_response(resp_payload, false,
            rpc::RpcError::kNeedRedirect, std::move(redirect));
    }

    // InternalRaftRequest is the command
    InternalRaftRequest internal_raft_request;
    internal_raft_request.mutable_kv_put()->Swap(&request);

    // Packaged as a log entry
    auto current_term = current_term_.load(std::memory_order_relaxed);
    auto match_index = logs_.last_log_index();
    // append size is the size that you just append, here is `1`
    auto append_size = 1;
    {
        auto entry = detail::produce_log_entry(current_term, match_index + 1, internal_raft_request.SerializeAsString());
        // Try to append and persist
        auto has_append = logs_.append_entry(std::move(entry));
        if (!has_append) {
            LOG_ERROR("{}", has_append.error());
            co_return detail::produce_kv_put_response(resp_payload, false,
                rpc::RpcError::kLogEntryAppendOrPersistFailed);
        }
    }

    // Update match_index and next_index
    match_index_[transport_.member_id()] = match_index + 1;
    next_index_[transport_.member_id()] = match_index_[transport_.member_id()] + 1;

    // Synchronize log entry to other nodes
    auto append_entries_request = produce_append_entries_request();
    auto* new_entry = append_entries_request.add_entries();
    new_entry->set_term(current_term);
    new_entry->set_index(match_index + 1);
    new_entry->set_command(internal_raft_request.SerializeAsString());

    // TODO: Wait for more applytask before synchronizing log entry to other nodes
    // Save the internal raft request as applytask and wait for processing
    state_machine_.produce_apply_task(detail::ApplyTask{session_id, request_id, std::move(internal_raft_request)});

    co_await transport_.broadcast_append_entries_request(std::move(append_entries_request),
        [this, match_index, append_size](std::string_view resp_payload) -> kosio::async::Task<> {
            co_await this->handle_append_entries_response(resp_payload, match_index, append_size);
    });

    // Critical, return 0 tells the rpc provider does not immediate send
    // response which should be sent after it was applied.
    co_return 0;
}

auto foskv::raft::RaftNode::handle_kv_get_request(std::string_view req_payload, std::span<char> resp_payload,
    uint64_t session_id, uint64_t request_id) -> kosio::async::Task<Result<std::size_t>> {
    kv::GetRequest request;
    if (!request.ParseFromArray(req_payload.data(), req_payload.size())) {
        co_return std::unexpected{make_error(Error::kKVGetRequestParseFailed)};
    }

    co_await mutex_.lock();
    std::lock_guard lock(mutex_, std::adopt_lock);

    // Return redirect response
    if (role_.load(std::memory_order_relaxed) != kLeader) {
        std::optional<rpc::Redirect> redirect{std::nullopt};
        // Try to get redirect info
        if (leader_id_) {
            auto leader = transport_.config_.peers_.find(leader_id_.value());
            if (leader != transport_.config_.peers_.end()) {
                auto host = leader->second.host();
                auto port = leader->second.port();
                redirect = detail::produce_redirect(std::move(host), port);
            }
        }
        // Tell the client that it need to redirect to the leader
        // If 'redirect' = 'std::nullopt', it means that
        // the client needs to determine the redirect address itself
        co_return detail::produce_kv_get_response(resp_payload, false,
            rpc::RpcError::kNeedRedirect, std::nullopt, std::move(redirect));
    }

    // InternalRaftRequest is the command
    InternalRaftRequest internal_raft_request;
    internal_raft_request.mutable_kv_get()->Swap(&request);

    // Packaged as a log entry
    auto current_term = current_term_.load(std::memory_order_relaxed);
    auto match_index = logs_.last_log_index();
    // append size is the size that you just append, here is `1`
    auto append_size = 1;
    {
        auto entry = detail::produce_log_entry(current_term, match_index + append_size, internal_raft_request.SerializeAsString());
        // Try to append and persist
        auto has_append = logs_.append_entry(std::move(entry));
        if (!has_append) {
            LOG_ERROR("{}", has_append.error());
            co_return detail::produce_kv_get_response(resp_payload, false, rpc::RpcError::kLogEntryAppendOrPersistFailed);
        }
    }

    // Update match_index and next_index
    match_index_[transport_.member_id()] = match_index + append_size;
    next_index_[transport_.member_id()] = match_index_[transport_.member_id()] + 1;

    // Synchronize log entry to other nodes
    auto append_entries_request = produce_append_entries_request();
    auto* new_entry = append_entries_request.add_entries();
    new_entry->set_term(current_term);
    new_entry->set_index(match_index + append_size);
    new_entry->set_command(internal_raft_request.SerializeAsString());

    // TODO: Wait for more applytask before synchronizing log entry to other nodes
    // Save the internal raft request as applytask and wait for processing
    state_machine_.produce_apply_task(detail::ApplyTask{session_id, request_id, std::move(internal_raft_request)});

    co_await transport_.broadcast_append_entries_request(std::move(append_entries_request),
        [this, match_index, append_size](std::string_view resp_payload) -> kosio::async::Task<> {
            co_await this->handle_append_entries_response(resp_payload, match_index, append_size);
    });

    // Critical, return 0 tells the rpc provider does not immediate send
    // response which should be sent after it was applied.
    co_return 0;
}

auto foskv::raft::RaftNode::handle_kv_delete_request(std::string_view req_payload, std::span<char> resp_payload,
    uint64_t session_id, uint64_t request_id) -> kosio::async::Task<Result<std::size_t>> {
    kv::DeleteRequest request;
    if (!request.ParseFromArray(req_payload.data(), req_payload.size())) {
        co_return std::unexpected{make_error(Error::kKVDeleteRequestParseFailed)};
    }

    co_await mutex_.lock();
    std::lock_guard lock(mutex_, std::adopt_lock);

    // Return redirect response
    if (role_.load(std::memory_order_relaxed) != kLeader) {
        std::optional<rpc::Redirect> redirect{std::nullopt};
        // Try to get redirect info
        if (leader_id_) {
            auto leader = transport_.config_.peers_.find(leader_id_.value());
            if (leader != transport_.config_.peers_.end()) {
                auto host = leader->second.host();
                auto port = leader->second.port();
                redirect = detail::produce_redirect(std::move(host), port);
            }
        }
        // Tell the client that it need to redirect to the leader
        // If 'redirect' = 'std::nullopt', it means that
        // the client needs to determine the redirect address itself
        co_return detail::produce_kv_delete_response(resp_payload, false,
            rpc::RpcError::kNeedRedirect, std::move(redirect));
    }

    // InternalRaftRequest is the command
    InternalRaftRequest internal_raft_request;
    internal_raft_request.mutable_kv_delete()->Swap(&request);

    // Packaged as a log entry
    auto current_term = current_term_.load(std::memory_order_relaxed);
    auto match_index = logs_.last_log_index();
    // append size is the size that you just append, here is `1`
    auto append_size = 1;
    {
        auto entry = detail::produce_log_entry(current_term, match_index + append_size, internal_raft_request.SerializeAsString());
        // Try to append and persist
        auto has_append = logs_.append_entry(std::move(entry));
        if (!has_append) {
            LOG_ERROR("{}", has_append.error());
            co_return detail::produce_kv_delete_response(resp_payload, false,
                rpc::RpcError::kLogEntryAppendOrPersistFailed);
        }
    }

    // Update match_index and next_index
    match_index_[transport_.member_id()] = match_index + append_size;
    next_index_[transport_.member_id()] = match_index_[transport_.member_id()] + 1;

    // Synchronize log entry to other nodes
    auto append_entries_request = produce_append_entries_request();
    auto* new_entry = append_entries_request.add_entries();
    new_entry->set_term(current_term);
    new_entry->set_index(match_index + append_size);
    new_entry->set_command(internal_raft_request.SerializeAsString());

    // TODO: Wait for more applytask before synchronizing log entry to other nodes
    // Save the internal raft request as applytask and wait for processing
    state_machine_.produce_apply_task(detail::ApplyTask{session_id, request_id, std::move(internal_raft_request)});

    co_await transport_.broadcast_append_entries_request(std::move(append_entries_request),
        [this, match_index, append_size](std::string_view resp_payload) -> kosio::async::Task<> {
            co_await this->handle_append_entries_response(resp_payload, match_index, append_size);
    });

    // Critical, return 0 tells the rpc provider does not immediate send
    // response which should be sent after it was applied.
    co_return 0;
}

auto foskv::raft::RaftNode::produce_response_header()
const noexcept -> ResponseHeader {
    ResponseHeader header;
    header.set_cluster_id(transport_.cluster_id());
    header.set_member_id(transport_.member_id());
    header.set_term(current_term_.load(std::memory_order_relaxed));
    return header;
}

auto foskv::raft::RaftNode::produce_request_vote_request() const noexcept -> RequestVoteRequest {
    RequestVoteRequest request;
    request.set_candidate_id(transport_.member_id());
    request.set_term(current_term_.load(std::memory_order_relaxed));
    auto last_log_index = logs_.last_log_index();
    auto last_log_term = logs_.last_log_term();
    request.set_last_log_index(last_log_index);
    request.set_last_log_term(last_log_term);
    return request;
}

auto foskv::raft::RaftNode::produce_request_vote_response(bool vote_granted, std::span<char> resp_payload)
const noexcept -> Result<std::size_t> {
    RequestVoteResponse response;
    auto header = produce_response_header();
    response.mutable_header()->Swap(&header);
    response.set_vote_granted(vote_granted);
    auto resp_payload_size = response.ByteSizeLong();
    if (!response.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
        return std::unexpected{make_error(Error::kRequestVoteResponseSerializeFailed)};
    }
    return resp_payload_size;
}

auto foskv::raft::RaftNode::produce_append_entries_request()
const noexcept -> AppendEntriesRequest {
    AppendEntriesRequest request;
    request.set_leader_id(transport_.member_id());
    request.set_term(current_term_.load(std::memory_order_relaxed));
    auto prev_log_index = logs_.prev_log_index();
    auto prev_log_term = logs_.prev_log_term();
    request.set_prev_log_index(prev_log_index);
    request.set_prev_log_term(prev_log_term);
    request.set_leader_commit(commit_index_);
    return request;
}

auto foskv::raft::RaftNode::produce_append_entries_response(bool success, std::span<char> resp_payload)
const noexcept -> Result<std::size_t> {
    AppendEntriesResponse response;
    auto header = produce_response_header();
    response.mutable_header()->Swap(&header);
    response.set_success(success);
    auto resp_payload_size = response.ByteSizeLong();
    if (!response.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
        return std::unexpected{make_error(Error::kAppendEntriesResponseSerializeFailed)};
    }
    return resp_payload_size;
}

auto foskv::raft::RaftNode::produce_install_snapshot_request(uint64_t last_included_index, uint64_t last_included_term,
    uint64_t offset, std::string &&data, bool done) const noexcept -> InstallSnapshotRequest {
    InstallSnapshotRequest request;
    request.set_leader_id(transport_.member_id());
    request.set_term(current_term_.load(std::memory_order_relaxed));
    request.set_last_included_index(last_included_index);
    request.set_last_included_term(last_included_term);
    request.set_offset(offset);
    request.set_data(std::move(data));
    request.set_done(done);
    return request;
}

auto foskv::raft::RaftNode::produce_install_snapshot_response(std::span<char> resp_payload)
const noexcept -> Result<std::size_t> {
    InstallSnapshotResponse response;
    auto header = produce_response_header();
    response.mutable_header()->Swap(&header);
    auto resp_payload_size = response.ByteSizeLong();
    if (!response.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
        return std::unexpected{make_error(Error::kInstallSnapshotResponseSerializeFailed)};
    }
    return resp_payload_size;
}
