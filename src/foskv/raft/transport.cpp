#include "foskv/raft/transport.hpp"

#include <ranges>

foskv::raft::detail::Transport::Transport(RaftConfig&& config)
    : config_(std::move(config))
    , provider_(config_.addr_) {}

auto foskv::raft::detail::Transport::run() -> kosio::async::Task<Result<void>> {
    co_return co_await provider_.run();
}

auto foskv::raft::detail::Transport::broadcast_request_vote_request(RequestVoteRequest request,
    const Peer::RpcCallback& callback) -> kosio::async::Task<> {
    auto payload = request.SerializeAsString();
    for (auto& peer : config_.peers_ | std::views::values) {
        if (peer.member_id() != config_.member_id_) {
            co_await peer.request_vote(payload, callback);
        }
    }
}

auto foskv::raft::detail::Transport::broadcast_append_entries_request(AppendEntriesRequest request,
    const Peer::RpcCallback& callback) -> kosio::async::Task<> {
    auto payload = request.SerializeAsString();
    for (auto& peer : config_.peers_ | std::views::values) {
        if (peer.member_id() != config_.member_id_) {
            co_await peer.append_entries(payload, callback);
        }
    }
}

auto foskv::raft::detail::Transport::broadcast_install_snapshot_request(InstallSnapshotRequest request,
    const Peer::RpcCallback& callback) -> kosio::async::Task<> {
    auto payload = request.SerializeAsString();
    for (auto& peer : config_.peers_ | std::views::values) {
        if (peer.member_id() != config_.member_id_) {
            co_await peer.install_snapshot(payload, callback);
        }
    }
}
