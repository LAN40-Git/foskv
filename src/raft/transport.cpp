#include "foskv/raft/transport.hpp"

#include <ranges>

foskv::raft::Transport::Transport(const kosio::net::SocketAddr &addr)
    : rpc_provider_(addr) {
    // TODO: Load peers from file
}

foskv::raft::Transport::Transport(const kosio::net::SocketAddr &addr, PeerMap &&peers)
    : rpc_provider_(addr)
    , peers_(std::move(peers)) {}

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

auto foskv::raft::Transport::broadcase_request_vote(RequestVoteRequest&& request,
    rpc::RpcCallback&& callback) -> kosio::async::Task<> {
    // TODO: Optimize with buffer pools
    auto payload = request.SerializeAsString();
    for (auto& peer : peers_ | std::views::values) {
        co_await peer.request_vote(payload, std::move(callback));
    }
}

auto foskv::raft::Transport::broadcase_append_entries(RequestVoteRequest&& request,
    rpc::RpcCallback &&callback) -> kosio::async::Task<> {
    // TODO: Optimize with buffer pools
    auto payload = request.SerializeAsString();
    for (auto& peer : peers_ | std::views::values) {
        co_await peer.append_entries(payload, std::move(callback));
    }
}

auto foskv::raft::Transport::broadcase_install_snapshot(RequestVoteRequest&& request,
    rpc::RpcCallback &&callback) -> kosio::async::Task<> {
    // TODO: Optimize with buffer pools
    auto payload = request.SerializeAsString();
    for (auto& peer : peers_ | std::views::values) {
        co_await peer.install_snapshot(payload, std::move(callback));
    }
}
