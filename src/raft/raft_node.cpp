#include "foskv/raft/raft_node.hpp"
#include "kosio/common/util/random.hpp"

foskv::raft::RaftNode::RaftNode(uint64_t member_id, const kosio::net::SocketAddr &addr)
    : transport_(addr) {
    init();
}

foskv::raft::RaftNode::RaftNode(uint64_t member_id, const kosio::net::SocketAddr &addr, Transport::PeerMap &&peers)
    : transport_(addr, std::move(peers)) {
    init();
}

void foskv::raft::RaftNode::init() {
    transport_.rpc_provider_.register_invoke(RaftRpc::ServiceName, RaftRpc::RequestVote,
        [this](std::string_view payload, std::span<char> response) -> RpcResult<std::size_t> {
        return this->handle_request_vote_request(payload, response);
    });
    transport_.rpc_provider_.register_invoke(RaftRpc::ServiceName, RaftRpc::AppendEntries,
        [this](std::string_view payload, std::span<char> response) -> RpcResult<std::size_t> {
        return this->handle_append_entries_request(payload, response);
    });
    transport_.rpc_provider_.register_invoke(RaftRpc::ServiceName, RaftRpc::InstallSnapshot,
        [this](std::string_view payload, std::span<char> response) -> RpcResult<std::size_t> {
        return this->handle_install_snapshot_request(payload, response);
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
        auto timeout = fast_rand.rand_range(150, 300);
        co_await kosio::time::sleep(timeout);

        co_await mutex_.lock();
        std::lock_guard lock(mutex_, std::adopt_lock);

        // The time difference between the last RPC
        // and the current time exceeds the election timeout
        if (last_rpc_time_ + timeout < kosio::util::current_ms()) {
            RequestVoteRequest request;
            auto* header = request.mutable_header();
            header->set_term(current_term_);
            request.set_last_log_index(logs_.size());
            request.set_last_log_term(logs_.back().log_term());
            kosio::spawn(transport_.broadcase_request_vote(std::move(request),
            [this](RpcResult<std::string_view> has_response) -> kosio::async::Task<> {
                if (!has_response) [[unlikely]] {
                    LOG_ERROR("{}", has_response.error());
                    co_return;
                }

                auto response_str = has_response.value();

                RequestVoteResponse response;
                if (!response.ParseFromArray(response_str.data(), response_str.size())) [[unlikely]] {
                    LOG_ERROR("Failed to parse request vote response");
                    co_return;
                }

                auto resp_term = response.term();
                auto vote_granted = response.vote_granted();
                // Votes for local raftnode at current term
                static std::size_t votes{0};

                co_await mutex_.lock();
                std::lock_guard lock(mutex_, std::adopt_lock);

                if (resp_term < current_term_) {
                    co_return;
                }

                if (resp_term > current_term_) {
                    increase_term_to(current_term_);
                    role_.store(kFollower, std::memory_order_relaxed);
                    votes = 0;
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
}

auto foskv::raft::RaftNode::start_heartbeat_timeout() -> kosio::async::Task<> {
    while (!is_shutdown_.load(std::memory_order_relaxed)) {
        // 50ms heartbeat timeout
        co_await kosio::time::sleep(50);

        if (role_.load(std::memory_order_relaxed) == kLeader) {
            continue;
        }

        co_await mutex_.lock();
        std::lock_guard lock(mutex_, std::adopt_lock);

        // Check again
        if (role_.load(std::memory_order_relaxed) == kLeader) {
            continue;
        }

        AppendEntriesRequest request;
        auto* header = request.mutable_header();
        header->set_term(current_term_);
        auto prev_log_index = logs_.size() - 1;
        request.set_prev_log_index(prev_log_index);
        if (prev_log_index != 0) {
            request.set_prev_log_term(logs_[prev_log_index].log_term());
        }

    }
}

void foskv::raft::RaftNode::increase_term_to(uint64_t term) {
    current_term_ = term;
    voted_for_ = std::nullopt;
}

void foskv::raft::RaftNode::become_leader() {
    role_.store(kLeader, std::memory_order_relaxed);
}

auto foskv::raft::RaftNode::handle_request_vote_request(std::string_view payload, std::span<char> response)
-> RpcResult<std::size_t> {

}

auto foskv::raft::RaftNode::handle_append_entries_request(std::string_view payload, std::span<char> response)
-> RpcResult<std::size_t> {

}

auto foskv::raft::RaftNode::handle_install_snapshot_request(std::string_view payload, std::span<char> response)
-> RpcResult<std::size_t> {

}
