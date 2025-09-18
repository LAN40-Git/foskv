#include "foskv/raft/raft_node.hpp"
#include "kosio/common/util/random.hpp"

foskv::raft::RaftNode::RaftNode(const Config& config) {

}

auto foskv::raft::RaftNode::create(std::string_view config_file_path) -> RaftResult<std::unique_ptr<RaftNode>> {
    // Load config
    auto has_config = Config::load(config_file_path);
    if (!has_config) {
        return std::unexpected{has_config.error()};
    }
    // Load persist state

}

void foskv::raft::RaftNode::init() {
    // TODO: Load the configuration from file

    // Register invokes
    transport_.rpc_provider_.register_invoke(RaftRpc::ServiceName, RaftRpc::RequestVote,
        [this](std::string_view req_payload, std::span<char> resp_payload) -> kosio::async::Task<RpcResult<std::size_t>>  {
        co_return co_await this->handle_request_vote_request(req_payload, resp_payload);
    });
    transport_.rpc_provider_.register_invoke(RaftRpc::ServiceName, RaftRpc::AppendEntries,
        [this](std::string_view req_payload, std::span<char> resp_payload) -> kosio::async::Task<RpcResult<std::size_t>> {
        co_return co_await this->handle_append_entries_request(req_payload, resp_payload);
    });
    transport_.rpc_provider_.register_invoke(RaftRpc::ServiceName, RaftRpc::InstallSnapshot,
        [this](std::string_view req_payload, std::span<char> resp_payload) -> kosio::async::Task<RpcResult<std::size_t>> {
        co_return co_await this->handle_install_snapshot_request(req_payload, resp_payload);
    });
}

auto foskv::raft::RaftNode::run() -> kosio::async::Task<> {
    kosio::spawn(transport_.run());
    kosio::spawn(start_election_timeout());
    kosio::spawn(start_heartbeat_timeout());
    co_await kosio::signal::ctrl_c();
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
        auto* header = request.mutable_header();
        header->set_member_id(transport_.member_id_);
        header->set_term(current_term);
        auto last_log_index = logs_.size() - 1;
        auto last_log_term = (last_log_index == 0) ? 0 : logs_[last_log_index].log_term();
        request.set_last_log_index(last_log_index);
        request.set_last_log_term(last_log_term);
        kosio::spawn(transport_.broadcast_request_vote(std::move(request),
        [this](std::string_view resp_payload) -> kosio::async::Task<> {
                RequestVoteResponse response;
                if (!response.ParseFromArray(resp_payload.data(), resp_payload.size())) [[unlikely]] {
                    LOG_ERROR("Failed to parse request vote response");
                    co_return;
                }

                auto resp_term = response.term();
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
                    if (votes > transport_.peers_.size() / 2) {
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
        auto* header = request.mutable_header();
        header->set_member_id(transport_.member_id_);
        header->set_term(current_term_.load(std::memory_order_relaxed));
        auto prev_log_index = logs_.size() - 1;
        auto prev_log_term = (prev_log_index == 0) ? 0 : logs_[prev_log_index].log_term();
        request.set_prev_log_index(prev_log_index);
        request.set_prev_log_term(prev_log_term);
        request.set_leader_commit(commit_index_);
        kosio::spawn(transport_.broadcast_append_entries(std::move(request),
            [this](std::string_view resp_payload) -> kosio::async::Task<> {
                AppendEntriesResponse response;
                if (!response.ParseFromArray(resp_payload.data(), resp_payload.size())) [[unlikely]] {
                    LOG_ERROR("Failed to parse request vote response");
                    co_return;
                }

                auto resp_term = response.term();
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
-> kosio::async::Task<RpcResult<std::size_t>> {
    RequestVoteRequest request;
    if (!request.ParseFromArray(req_payload.data(), req_payload.size())) [[unlikely]] {
        co_return std::unexpected{make_rpc_error(RpcError::kParseFailed)};
    }

    std::size_t resp_payload_size;
    auto header = request.header();
    auto candidate_id = header.member_id();
    auto req_term = header.term();
    auto last_log_index = request.last_log_index();
    auto last_log_term = request.last_log_term();

    if (req_term < current_term_.load(std::memory_order_relaxed)) {
        RequestVoteResponse response;
        response.set_term(current_term_.load(std::memory_order_relaxed));
        response.set_vote_granted(false);
        resp_payload_size = response.ByteSizeLong();
        if (!response.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
            co_return std::unexpected{make_rpc_error(RpcError::kSerializeFailed)};
        }
        co_return resp_payload_size;
    }

    co_await mutex_.lock();
    std::lock_guard lock(mutex_, std::adopt_lock);

    // Check again
    auto current_term = current_term_.load(std::memory_order_relaxed);
    if (req_term < current_term) {
        RequestVoteResponse response;
        response.set_term(current_term);
        response.set_vote_granted(false);
        resp_payload_size = response.ByteSizeLong();
        if (!response.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
            co_return std::unexpected{make_rpc_error(RpcError::kSerializeFailed)};
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

    if (last_log_index > logs_.size() - 1 ||
        (last_log_index == logs_.size() - 1 && last_log_term == logs_[last_log_index].log_term())) {
        up_to_date_log = true;
    }

    RequestVoteResponse response;
    response.set_term(current_term);
    response.set_vote_granted(can_vote && up_to_date_log);
    resp_payload_size = response.ByteSizeLong();
    if (!response.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
        co_return std::unexpected{make_rpc_error(RpcError::kSerializeFailed)};
    }
    co_return resp_payload_size;
}

auto foskv::raft::RaftNode::handle_append_entries_request(std::string_view req_payload, std::span<char> resp_payload)
-> kosio::async::Task<RpcResult<std::size_t>> {
    AppendEntriesRequest request;
    if (!request.ParseFromArray(req_payload.data(), req_payload.size())) [[unlikely]] {
        co_return std::unexpected{make_rpc_error(RpcError::kParseFailed)};
    }

    std::size_t resp_payload_size;
    auto header = request.header();
    auto leader_id = header.member_id();
    auto req_term = header.term();
    auto prev_log_index = request.prev_log_index();
    auto prev_log_term = request.prev_log_term();
    auto entries = request.entries();
    auto leader_commit = request.leader_commit();

    if (req_term < current_term_.load(std::memory_order_relaxed)) {
        AppendEntriesResponse response;
        response.set_term(current_term_.load(std::memory_order_relaxed));
        response.set_success(false);
        resp_payload_size = response.ByteSizeLong();
        if (!response.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
            co_return std::unexpected{make_rpc_error(RpcError::kSerializeFailed)};
        }
        co_return resp_payload_size;
    }

    co_await mutex_.lock();
    std::lock_guard lock(mutex_, std::adopt_lock);

    // Check again
    auto current_term = current_term_.load(std::memory_order_relaxed);
    if (req_term < current_term) {
        AppendEntriesResponse response;
        response.set_term(current_term);
        response.set_success(false);
        resp_payload_size = response.ByteSizeLong();
        if (!response.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
            co_return std::unexpected{make_rpc_error(RpcError::kSerializeFailed)};
        }
        co_return resp_payload_size;
    }

    if (req_term > current_term) {
        current_term = req_term;
        increase_term_to(req_term);
        role_.store(kFollower, std::memory_order_relaxed);
        last_reset_time_.store(kosio::util::current_ms(), std::memory_order_relaxed);
    }

    auto last_log_index = logs_.size() - 1;

    // Heartbeat
    if (entries.empty()) {
        last_reset_time_.store(kosio::util::current_ms(), std::memory_order_relaxed);
        commit_index_ = std::min(last_log_index, leader_commit);
        AppendEntriesResponse response;
        response.set_term(current_term);
        response.set_success(true);
        resp_payload_size = response.ByteSizeLong();
        if (!response.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
            co_return std::unexpected{make_rpc_error(RpcError::kSerializeFailed)};
        }
        co_return resp_payload_size;
    }

    if (prev_log_index > last_log_index) {
        // Our log is too old, return false
        AppendEntriesResponse response;
        response.set_term(current_term);
        response.set_success(false);
        resp_payload_size = response.ByteSizeLong();
        if (!response.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
            co_return std::unexpected{make_rpc_error(RpcError::kSerializeFailed)};
        }
        co_return resp_payload_size;
    }

    if (logs_[prev_log_index].log_term() != prev_log_term) {
        logs_.erase(logs_.begin() + prev_log_index, logs_.end());
    }

    for (auto& entry : entries) {
        logs_.push_back(std::move(entry));
    }
    commit_index_ = std::min(last_log_index, leader_commit);

    AppendEntriesResponse response;
    response.set_term(current_term);
    response.set_success(true);
    resp_payload_size = response.ByteSizeLong();
    if (!response.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
        co_return std::unexpected{make_rpc_error(RpcError::kSerializeFailed)};
    }
    co_return resp_payload_size;
}

auto foskv::raft::RaftNode::handle_install_snapshot_request(std::string_view req_payload, std::span<char> resp_payload)
-> kosio::async::Task<RpcResult<std::size_t>> {
    InstallSnapshotRequest request;
    if (!request.ParseFromArray(req_payload.data(), req_payload.size())) [[unlikely]] {
        co_return std::unexpected{make_rpc_error(RpcError::kParseFailed)};
    }


}
