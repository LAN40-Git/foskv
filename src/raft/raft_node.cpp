#include "foskv/raft/raft_node.hpp"
#include "kosio/common/util/random.hpp"

foskv::raft::RaftNode::RaftNode(std::string_view host, uint16_t port)
    : transport_(host, port) {}

foskv::raft::RaftNode::RaftNode(std::string_view host, uint16_t port, Transport::PeerMap &&peers)
    : transport_(host, port, std::move(peers)) {}

auto foskv::raft::RaftNode::start_election_timeout() -> kosio::async::Task<> {
    kosio::util::FastRand fast_rand;
    while (true) {
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
            header->set_term(current_term);
            request.set_last_log_index(logs.size());
            request.set_last_log_term(logs.back().log_term());
            // Broadcast request vote request
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

                if (resp_term < current_term) {
                    co_return;
                }

                if (resp_term > current_term) {
                    increase_term_to(current_term);
                    role = kFollower; votes = 0;
                    co_return;
                }

            }));
        }
    }
}

auto foskv::raft::RaftNode::start_heartbeat_timeout() -> kosio::async::Task<> {
    // 50ms heartbeat timeout
    co_await kosio::time::sleep(50);

    co_await mutex_.lock();
    std::lock_guard lock(mutex_, std::adopt_lock);
}

void foskv::raft::RaftNode::increase_term_to(uint64_t term) {
    current_term = term;
    voted_for = std::nullopt;
}

auto foskv::raft::RaftNode::handle_request_vote_request()
-> RpcResult<std::size_t> {

}

auto foskv::raft::RaftNode::handle_append_entries_request()
-> RpcResult<std::size_t> {

}

auto foskv::raft::RaftNode::handle_install_snapshot_request()
-> RpcResult<std::size_t> {

}
