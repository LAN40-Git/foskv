#include "foskv/raft/raft.hpp"
#include "kosio/common/util/random.hpp"

foskv::raft::RaftNode::RaftNode(std::string_view host, uint16_t port)
    : transport_(host, port) {}

foskv::raft::RaftNode::RaftNode(std::string_view host, uint16_t port, Transport::PeerMap &&peers)
    : transport_(host, port, std::move(peers)) {}

auto foskv::raft::RaftNode::election_event_loop() -> kosio::async::Task<> {
    // 150-300ms 选举超时
    thread_local kosio::util::FastRand fast_rand;
    auto timeout = fast_rand.rand_range(150, 300);
    co_await kosio::time::sleep(timeout);

    co_await mutex_.lock();
    std::lock_guard lock(mutex_, std::adopt_lock);

    // The time difference between the last RPC
    // and the current time exceeds the election timeout
    if (last_rpc_time_ + timeout < kosio::util::current_ms()) {
        // DO election
        do_election();
    }
}

auto foskv::raft::RaftNode::heartbeat_event_loop() -> kosio::async::Task<> {
    // 50ms 心跳超时
    co_await kosio::time::sleep(50);

    co_await mutex_.lock();
    std::lock_guard lock(mutex_, std::adopt_lock);

}

void foskv::raft::RaftNode::do_election() {

}

void foskv::raft::RaftNode::do_heartbeat() {

}

auto foskv::raft::RaftNode::request_vote_callback(std::string_view payload,
    std::span<char> response) -> kosio::async::Task<> {
    co_await mutex_.lock();
    std::lock_guard lock(mutex_, std::adopt_lock);

}

auto foskv::raft::RaftNode::append_entries_callback(std::string_view payload,
    std::span<char> response) -> kosio::async::Task<> {
    co_await mutex_.lock();
    std::lock_guard lock(mutex_, std::adopt_lock);

}

auto foskv::raft::RaftNode::install_snapshot_callback(std::string_view payload,
    std::span<char> response) -> kosio::async::Task<> {
    co_await mutex_.lock();
    std::lock_guard lock(mutex_, std::adopt_lock);

}
