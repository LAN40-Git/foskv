#include "foskv/raft/transport.hpp"

#include <ranges>

foskv::raft::Transport::Transport(uint64_t member_id, const kosio::net::SocketAddr &addr)
    : member_id_(member_id)
    , rpc_provider_(addr) {}

foskv::raft::Transport::Transport(uint64_t member_id, const kosio::net::SocketAddr &addr, PeerMap &&peers)
    : member_id_(member_id)
    , peers_(std::move(peers))
    , rpc_provider_(addr) {}

auto foskv::raft::Transport::run() -> kosio::async::Task<> {
    std::size_t count{0};
    while (true) {
        auto ret = co_await rpc_provider_.run();
        if (!ret) [[unlikely]] {
            LOG_ERROR("{}", ret.error());
            count++;
        }
        if (count == 3) {
            LOG_ERROR("Failed to run after 3 attempts, exit.");
            break;
        }
    }
}

auto foskv::raft::Transport::broadcast_request_vote(RequestVoteRequest&& request,
    rpc::RpcCallback&& callback) -> kosio::async::Task<> {
    // TODO: Optimize with buffer pools
    auto payload = request.SerializeAsString();
    for (auto& peer : peers_ | std::views::values) {
        co_await peer.request_vote(payload, std::move(callback));
    }
}

auto foskv::raft::Transport::broadcast_append_entries(AppendEntriesRequest&& request,
    rpc::RpcCallback &&callback) -> kosio::async::Task<> {
    // TODO: Optimize with buffer pools
    auto payload = request.SerializeAsString();
    for (auto& peer : peers_ | std::views::values) {
        co_await peer.append_entries(payload, std::move(callback));
    }
}

auto foskv::raft::Transport::broadcast_install_snapshot(InstallSnapshotRequest&& request,
    rpc::RpcCallback &&callback) -> kosio::async::Task<> {
    // TODO: Optimize with buffer pools
    auto payload = request.SerializeAsString();
    for (auto& peer : peers_ | std::views::values) {
        co_await peer.install_snapshot(payload, std::move(callback));
    }
}
