#include "foskv/raft/raft.hpp"
#include "kosio/common/util/random.hpp"

auto foskv::raft::RaftNode::election_event_loop() -> kosio::async::Task<> {
    // 150-300ms 选举超时
    thread_local kosio::util::FastRand fast_rand;
    auto timeout = fast_rand.rand_range(150, 300);
    co_await kosio::time::sleep(timeout);

    co_await mutex_.lock();
    std::lock_guard lock(mutex_, std::adopt_lock);

    if (last_rpc_time_ + timeout < kosio::util::current_ms()) {
        do_election();
    }
}

auto foskv::raft::RaftNode::heartbeat_event_loop() -> kosio::async::Task<> {
    // 50ms 心跳超时
    co_await kosio::time::sleep(50);

    co_await mutex_.lock();
    std::lock_guard lock(mutex_, std::adopt_lock);

}
