#pragma once
#include <kosio/sync.hpp>
#include "foskv/raft/peer.hpp"

namespace foskv::raft {
class Transport {
    using PeerMap = std::unordered_map<kosio::net::SocketAddr, Peer>;

public:
    explicit Transport(std::string_view host, uint16_t port);
    explicit Transport(std::string_view host, uint16_t port, PeerMap&& peers);

public:
    void init();
    auto run() -> kosio::async::Task<>;
    auto broadcase_request_vote(RequestVoteRequest&& request, rpc::RpcCallback&& callback) -> kosio::async::Task<>;
    auto broadcase_append_entries(RequestVoteRequest&& request, rpc::RpcCallback&& callback) -> kosio::async::Task<>;
    auto broadcase_install_snapshot(RequestVoteRequest&& request, rpc::RpcCallback&& callback) -> kosio::async::Task<>;

private:
    rpc::RpcProvider   rpc_provider_;
    PeerMap            peers_;
};
} // namespace foskv::raft