#include "foskv/raft/transport.hpp"

#include <ranges>

foskv::raft::detail::Transport::Transport(Config&& config)
    : config_(std::move(config))
    , provider_(config_.local_addr_) {}

auto foskv::raft::detail::Transport::run() -> kosio::async::Task<kosio::Result<void>> {
    co_return co_await provider_.run();
}

auto foskv::raft::detail::Transport::broadcast_request_vote_request(RequestVoteRequest&& request,
    Peer::RpcCallback&& callback) -> kosio::async::Task<> {
    // TODO: Optimize with buffer pools
    auto payload = request.SerializeAsString();
    for (auto& peer : config_.peers_ | std::views::values) {
        co_await peer.request_vote(payload, std::move(callback));
    }
}

auto foskv::raft::detail::Transport::broadcast_append_entries_request(AppendEntriesRequest&& request,
    Peer::RpcCallback &&callback) -> kosio::async::Task<> {
    // TODO: Optimize with buffer pools
    auto payload = request.SerializeAsString();
    for (auto& peer : config_.peers_ | std::views::values) {
        co_await peer.append_entries(payload, std::move(callback));
    }
}

auto foskv::raft::detail::Transport::broadcast_install_snapshot_request(InstallSnapshotRequest&& request,
    Peer::RpcCallback &&callback) -> kosio::async::Task<> {
    // TODO: Optimize with buffer pools
    auto payload = request.SerializeAsString();
    for (auto& peer : config_.peers_ | std::views::values) {
        co_await peer.install_snapshot(payload, std::move(callback));
    }
}
