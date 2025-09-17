#pragma once
#include <kosio/sync.hpp>
#include "foskv/raft/peer.hpp"

namespace foskv::raft {
class RaftNode;
class Transport {
    friend class RaftNode;
    using PeerMap = std::unordered_map<kosio::net::SocketAddr, Peer>;

public:
    explicit Transport(uint64_t member_id, const kosio::net::SocketAddr &addr);
    explicit Transport(uint64_t member_id, const kosio::net::SocketAddr &addr, PeerMap&& peers);

public:
    auto run() -> kosio::async::Task<>;
    auto broadcase_request_vote(RequestVoteRequest&& request, rpc::RpcCallback&& callback) -> kosio::async::Task<>;
    auto broadcase_append_entries(RequestVoteRequest&& request, rpc::RpcCallback&& callback) -> kosio::async::Task<>;
    auto broadcase_install_snapshot(RequestVoteRequest&& request, rpc::RpcCallback&& callback) -> kosio::async::Task<>;

private:
    rpc::RpcProvider   rpc_provider_;
    PeerMap            peers_;
};
} // namespace foskv::raft