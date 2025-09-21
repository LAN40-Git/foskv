#include "foskv/raft/raft_node.hpp"
#include "kosio/common/util/random.hpp"

foskv::raft::RaftNode::RaftNode(RaftConfig&& config, detail::Persister&& persister, detail::StateMachine&& state_machine)
    : persister_(std::move(persister))
    , transport_(std::move(config))
    , state_machine_(std::move(state_machine)) {
    // Load Persist state
    PersistState state = persister_.load_state();
    current_term_.store(state.current_term());
    state.has_voted_for() ? voted_for_ = state.voted_for() : voted_for_ = std::nullopt;
    commit_index_ = state.commit_index();

    // Load Persist log entries
    logs_ = persister_.load_entries();

    // Register invokes
    using rpc::RaftService;
    using rpc::KVService;
    transport_.provider_.register_invoke(RaftService::ServiceName, RaftService::RequestVote,
        [this](std::string_view, uint64_t, std::string_view req_payload, std::span<char> resp_payload) -> kosio::async::Task<Result<std::size_t>>  {
        co_return co_await this->handle_request_vote_request(req_payload, resp_payload);
    });
    transport_.provider_.register_invoke(RaftService::ServiceName, RaftService::AppendEntries,
        [this](std::string_view, uint64_t, std::string_view req_payload, std::span<char> resp_payload) -> kosio::async::Task<Result<std::size_t>> {
        co_return co_await this->handle_append_entries_request(req_payload, resp_payload);
    });
    transport_.provider_.register_invoke(RaftService::ServiceName, RaftService::InstallSnapshot,
        [this](std::string_view, uint64_t, std::string_view req_payload, std::span<char> resp_payload) -> kosio::async::Task<Result<std::size_t>> {
        co_return co_await this->handle_install_snapshot_request(req_payload, resp_payload);
    });
}

auto foskv::raft::RaftNode::create(std::string_view config_path, std::string_view data_dir)
-> kosio::async::Task<Result<std::unique_ptr<RaftNode>>> {
    std::filesystem::path dir(data_dir);
    if (!std::filesystem::is_directory(data_dir)) {
        co_return std::unexpected{make_error(Error::kInvalidDataDirectory)};
    }

    // Load config
    auto has_config = co_await RaftConfig::load(config_path);
    if (!has_config) [[unlikely]] {
        co_return std::unexpected{has_config.error()};
    }

    // Create persister and load persistent
    auto has_persister = detail::Persister::create(data_dir);
    if (!has_persister) [[unlikely]] {
        co_return std::unexpected{has_persister.error()};
    }

    // Create state machine
    auto has_state_machine = detail::StateMachine::create(data_dir);
    if (!has_state_machine) {
        co_return std::unexpected{has_state_machine.error()};
    }
    co_return std::make_unique<RaftNode>(std::move(has_config.value()), std::move(has_persister.value()), std::move(has_state_machine.value()));
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

        RequestVoteRequest request = produce_request_vote_request();
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
                    votes = 0;
                    increase_term_to(resp_term);
                    role_.store(kFollower, std::memory_order_release);
                    last_reset_time_.store(kosio::util::current_ms(), std::memory_order_relaxed);
                    co_return;
                }

                if (role_.load(std::memory_order_acquire) == kLeader) {
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

        if (role_.load(std::memory_order_acquire) != kLeader) {
            continue;
        }

        co_await mutex_.lock();
        std::lock_guard lock(mutex_, std::adopt_lock);

        // Check again
        if (role_.load(std::memory_order_relaxed) != kLeader) {
            continue;
        }

        AppendEntriesRequest request = produce_append_entries_request();
        kosio::spawn(transport_.broadcast_append_entries_request(std::move(request),
            [this](std::string_view resp_payload) -> kosio::async::Task<> {
                AppendEntriesResponse response;
                if (!response.ParseFromArray(resp_payload.data(), resp_payload.size())) [[unlikely]] {
                    LOG_ERROR("Failed to parse request vote response");
                    co_return;
                }

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
                    increase_term_to(resp_term);
                    role_.store(kFollower, std::memory_order_release);
                    last_reset_time_.store(kosio::util::current_ms(), std::memory_order_relaxed);
                    co_return;
                }
        }));
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
}

auto foskv::raft::RaftNode::handle_request_vote_request(std::string_view req_payload, std::span<char> resp_payload)
-> kosio::async::Task<Result<std::size_t>> {
    RequestVoteRequest request;
    if (!request.ParseFromArray(req_payload.data(), req_payload.size())) [[unlikely]] {
        co_return std::unexpected{make_error(Error::kProtobufParseFailed)};
    }

    std::size_t resp_payload_size;
    auto candidate_id = request.candidate_id();
    auto req_term = request.term();
    auto last_log_index = request.last_log_index();
    auto last_log_term = request.last_log_term();

    if (req_term < current_term_.load(std::memory_order_acquire)) {
        RequestVoteResponse response;
        auto header = produce_response_header();
        response.set_allocated_header(&header);
        response.set_vote_granted(false);
        resp_payload_size = response.ByteSizeLong();
        if (!response.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
            co_return std::unexpected{make_error(Error::kProtobufSerializeFailed)};
        }
        co_return resp_payload_size;
    }

    co_await mutex_.lock();
    std::lock_guard lock(mutex_, std::adopt_lock);

    // Check again
    auto current_term = current_term_.load(std::memory_order_relaxed);
    if (req_term < current_term) {
        RequestVoteResponse response;
        auto header = produce_response_header();
        response.set_allocated_header(&header);
        response.set_vote_granted(false);
        resp_payload_size = response.ByteSizeLong();
        if (!response.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
            co_return std::unexpected{make_error(Error::kProtobufSerializeFailed)};
        }
        co_return resp_payload_size;
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
    auto header = produce_response_header();
    response.set_allocated_header(&header);
    response.set_vote_granted(can_vote && up_to_date_log);
    resp_payload_size = response.ByteSizeLong();
    if (!response.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
        co_return std::unexpected{make_error(Error::kProtobufSerializeFailed)};
    }
    co_return resp_payload_size;
}

auto foskv::raft::RaftNode::handle_append_entries_request(std::string_view req_payload, std::span<char> resp_payload)
-> kosio::async::Task<Result<std::size_t>> {
    AppendEntriesRequest request;
    if (!request.ParseFromArray(req_payload.data(), req_payload.size())) [[unlikely]] {
        co_return std::unexpected{make_error(Error::kProtobufParseFailed)};
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
        auto header = produce_response_header();
        response.set_allocated_header(&header);
        response.set_success(false);
        resp_payload_size = response.ByteSizeLong();
        if (!response.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
            co_return std::unexpected{make_error(Error::kProtobufSerializeFailed)};
        }
        co_return resp_payload_size;
    }

    co_await mutex_.lock();
    std::lock_guard lock(mutex_, std::adopt_lock);

    // Check again
    auto current_term = current_term_.load(std::memory_order_relaxed);
    if (req_term < current_term) {
        AppendEntriesResponse response;
        auto header = produce_response_header();
        response.set_allocated_header(&header);
        response.set_success(false);
        resp_payload_size = response.ByteSizeLong();
        if (!response.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
            co_return std::unexpected{make_error(Error::kProtobufSerializeFailed)};
        }
        co_return resp_payload_size;
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
        if (leader_commit > commit_index_) {
            commit_index_ = std::min(last_log_index, leader_commit);
        }
        AppendEntriesResponse response;
        auto header = produce_response_header();
        response.set_allocated_header(&header);
        response.set_success(true);
        resp_payload_size = response.ByteSizeLong();
        if (!response.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
            co_return std::unexpected{make_error(Error::kProtobufSerializeFailed)};
        }
        co_return resp_payload_size;
    }

    if (prev_log_index > last_log_index) {
        // Our log is too old, return false
        AppendEntriesResponse response;
        auto header = produce_response_header();
        response.set_allocated_header(&header);
        response.set_success(false);
        resp_payload_size = response.ByteSizeLong();
        if (!response.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
            co_return std::unexpected{make_error(Error::kProtobufSerializeFailed)};
        }
        co_return resp_payload_size;
    }

    if (logs_[prev_log_index].term() != prev_log_term) {
        logs_.erase(logs_.begin() + prev_log_index, logs_.end());
    }

    for (auto& entry : entries) {
        logs_.push_back(std::move(entry));
    }
    if (leader_commit > commit_index_) {
        commit_index_ = std::min(last_log_index, leader_commit);
    }

    AppendEntriesResponse response;
    auto header = produce_response_header();
    response.set_allocated_header(&header);
    response.set_success(true);
    resp_payload_size = response.ByteSizeLong();
    if (!response.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
        co_return std::unexpected{make_error(Error::kProtobufSerializeFailed)};
    }
    co_return resp_payload_size;
}

auto foskv::raft::RaftNode::handle_install_snapshot_request(std::string_view req_payload, std::span<char> resp_payload)
-> kosio::async::Task<Result<std::size_t>> {
    InstallSnapshotRequest request;
    if (!request.ParseFromArray(req_payload.data(), req_payload.size())) [[unlikely]] {
        co_return std::unexpected{make_error(Error::kProtobufParseFailed)};
    }

}

auto foskv::raft::RaftNode::append_entries_callback(RaftNode *node, uint64_t prev_log_index, std::size_t entries_size,
                                                    std::string_view resp_payload) -> kosio::async::Task<> {
    AppendEntriesResponse response;
    if (!response.ParseFromArray(resp_payload.data(), resp_payload.size())) [[unlikely]] {
        LOG_ERROR("Failed to parse request vote response");
        co_return;
    }

    auto cluster_id = response.header().cluster_id();
    auto member_id = response.header().member_id();
    auto resp_term = response.header().term();
    auto success = response.success();

    if (resp_term < node->current_term_.load(std::memory_order_acquire) ||
        node->role_.load(std::memory_order_acquire) != kLeader) {
        co_return;
        }

    co_await node->mutex_.lock();
    std::lock_guard lock(node->mutex_, std::adopt_lock);

    // Check again
    auto current_term = node->current_term_.load(std::memory_order_relaxed);
    if (resp_term < current_term ||
        node->role_.load(std::memory_order_relaxed) != kLeader) {
        co_return;
    }

    if (resp_term > current_term) {
        current_term = resp_term;
        node->increase_term_to(resp_term);
        node->role_.store(kFollower, std::memory_order_acquire);
        node->last_reset_time_.store(kosio::util::current_ms(), std::memory_order_relaxed);
        co_return;
    }

    if (success) {
        // Update next_index_ and match_index_ for follower
        node->match_index_[member_id] = prev_log_index + entries_size;
        node->next_index_[member_id] = node->match_index_[member_id] + 1;

        // node->try_commit_entries();
        // node->persist();
        // co_await node->apply_commited_entries();
    } else {
        if (node->next_index_[member_id] > 1) {
            node->next_index_[member_id]--;
        }
        // TODO: Handle conflict
    }
}

auto foskv::raft::RaftNode::produce_response_header()
const noexcept -> ResponseHeader {
    ResponseHeader header;
    header.set_cluster_id(transport_.cluster_id());
    header.set_member_id(transport_.member_id());
    header.set_term(current_term_.load(std::memory_order_relaxed));
    return header;
}

auto foskv::raft::RaftNode::produce_internal_response_header(bool success, int error_code, std::optional<rpc::Redirect> redirect)
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

auto foskv::raft::RaftNode::produce_request_vote_request() const noexcept -> RequestVoteRequest {
    RequestVoteRequest request;
    request.set_candidate_id(transport_.member_id());
    request.set_term(current_term_.load(std::memory_order_relaxed));
    auto last_log_index = logs_.size();
    auto last_log_term = (last_log_index == 0) ? 0 : logs_[last_log_index].term();
    request.set_last_log_index(last_log_index);
    request.set_last_log_term(last_log_term);
    return request;
}

auto foskv::raft::RaftNode::produce_append_entries_request()
const noexcept -> AppendEntriesRequest {
    AppendEntriesRequest request;
    request.set_leader_id(transport_.member_id());
    request.set_term(current_term_.load(std::memory_order_relaxed));
    auto prev_log_index = logs_.size() - 1;
    auto prev_log_term = (prev_log_index == 0) ? 0 : logs_[prev_log_index].term();
    request.set_prev_log_index(prev_log_index);
    request.set_prev_log_term(prev_log_term);
    request.set_leader_commit(commit_index_);
    return request;
}
