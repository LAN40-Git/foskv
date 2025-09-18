#include "foskv/raft/transport.hpp"

#include <ranges>

foskv::raft::detail::Transport::Transport(Config&& config)
    : config_(std::move(config))
    , provider_(config_.local_addr_) {}

foskv::raft::detail::Transport::Transport(Transport &&other) noexcept
    : config_(std::move(other.config_))
    , provider_(std::move(other.provider_)) {}

auto foskv::raft::detail::Transport::operator=(Transport &&other) noexcept -> Transport & {
    config_ = std::move(other.config_);
    provider_ = std::move(other.provider_);
    return *this;
}

auto foskv::raft::detail::Transport::run() -> kosio::async::Task<> {
    try {
        co_await provider_.run();
    } catch (...) {
        throw;
    }
}

auto foskv::raft::detail::Transport::broadcast_request_vote(RequestVoteRequest&& request,
    rpc::RpcCallback&& callback) -> kosio::async::Task<> {
    // TODO: Optimize with buffer pools
    auto payload = request.SerializeAsString();
    for (auto& peer : config_.peers_ | std::views::values) {
        co_await peer.request_vote(payload, std::move(callback));
    }
}

auto foskv::raft::detail::Transport::broadcast_append_entries(AppendEntriesRequest&& request,
    rpc::RpcCallback &&callback) -> kosio::async::Task<> {
    // TODO: Optimize with buffer pools
    auto payload = request.SerializeAsString();
    for (auto& peer : config_.peers_ | std::views::values) {
        co_await peer.append_entries(payload, std::move(callback));
    }
}

auto foskv::raft::detail::Transport::broadcast_install_snapshot(InstallSnapshotRequest&& request,
    rpc::RpcCallback &&callback) -> kosio::async::Task<> {
    // TODO: Optimize with buffer pools
    auto payload = request.SerializeAsString();
    for (auto& peer : config_.peers_ | std::views::values) {
        co_await peer.install_snapshot(payload, std::move(callback));
    }
}
