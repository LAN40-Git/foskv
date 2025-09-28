#include "foskv/raft/transport.hpp"

#include <ranges>

foskv::raft::detail::Transport::Transport(RaftConfig&& config, std::unique_ptr<rpc::RpcProvider> provider)
    : config_(std::move(config))
    , provider_(std::move(provider)) {}

auto foskv::raft::detail::Transport::create(RaftConfig &&config) -> kosio::async::Task<Result<Transport>> {
    auto has_provider = co_await rpc::RpcProvider::create(config.addr_);
    if (!has_provider) {
        co_return std::unexpected{has_provider.error()};
    }
    co_return Transport{std::move(config), std::move(has_provider.value())};
}

auto foskv::raft::detail::Transport::run() const -> kosio::async::Task<Result<void>> {
    co_return co_await provider_->run();
}

auto foskv::raft::detail::Transport::shutdown() const -> kosio::async::Task<> {
    co_await provider_->shutdown();
}

auto foskv::raft::detail::Transport::single_append_entries_request(
    uint64_t to_member_id, AppendEntriesRequest request, Peer::RpcCallback callback) -> kosio::async::Task<> {
    if (to_member_id == this->member_id()) [[unlikely]] {
        co_return;
    }
    auto it = config_.peers_.find(to_member_id);
    if (it != config_.peers_.end()) {
        auto payload = request.SerializeAsString();
        co_await it->second.append_entries(payload, callback);
    }
}

auto foskv::raft::detail::Transport::broadcast_request_vote_request(
    RequestVoteRequest request, Peer::RpcCallback callback) -> kosio::async::Task<> {
    auto payload = request.SerializeAsString();
    for (auto& peer : config_.peers_ | std::views::values) {
        if (peer.member_id() != this->member_id()) {
            co_await peer.request_vote(payload, callback);
        }
    }
}

auto foskv::raft::detail::Transport::broadcast_append_entries_request(AppendEntriesRequest request,
    Peer::RpcCallback callback) -> kosio::async::Task<> {
    auto payload = request.SerializeAsString();
    for (auto& peer : config_.peers_ | std::views::values) {
        if (peer.member_id() != config_.member_id_) {
            co_await peer.append_entries(payload, callback);
        }
    }
}

auto foskv::raft::detail::Transport::broadcast_install_snapshot_request(InstallSnapshotRequest request,
    Peer::RpcCallback callback) -> kosio::async::Task<> {
    auto payload = request.SerializeAsString();
    for (auto& peer : config_.peers_ | std::views::values) {
        if (peer.member_id() != member_id()) {
            co_await peer.install_snapshot(payload, callback);
        }
    }
}
