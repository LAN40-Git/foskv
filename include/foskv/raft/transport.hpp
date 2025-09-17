#pragma once
#include <kosio/sync.hpp>
#include "foskv/raft/peer.hpp"
#include "foskv/rpc/provider.hpp"

namespace foskv::raft {
class Transport {
public:
    using PeerMap = std::unordered_map<kosio::net::SocketAddr, Peer>;

public:
    explicit Transport(std::string_view host, uint16_t port);
    explicit Transport(std::string_view host, uint16_t port, PeerMap&& peers);

public:
    auto run() -> kosio::async::Task<>;
    [[REMEMBER_CO_AWAIT]]
    auto broadcase_request_vote(std::string_view payload, rpc::RpcCallback&& callback) -> kosio::async::Task<>;
    [[REMEMBER_CO_AWAIT]]
    auto broadcase_append_entries(std::string_view payload, rpc::RpcCallback&& callback) -> kosio::async::Task<>;
    [[REMEMBER_CO_AWAIT]]
    auto broadcase_install_snapshot(std::string_view payload, rpc::RpcCallback&& callback) -> kosio::async::Task<>;

private:
    rpc::RpcProvider   rpc_provider_;
    PeerMap            peers_;
};
} // namespace foskv::raft