#pragma once
#include <kosio/sync.hpp>
#include "foskv/raft/peer.hpp"
#include "foskv/rpc/provider.hpp"

namespace foskv::raft {
class RaftNode;

class Transport {
    friend class RaftNode;
    using PeerMap = std::unordered_map<kosio::net::SocketAddr, Peer>;
public:
    explicit Transport(std::string_view host, uint16_t port);
    explicit Transport(std::string_view host, uint16_t port, PeerMap&& peers);

public:
    auto run() -> kosio::async::Task<>;

private:
    rpc::RpcProvider rpc_provider_;
    PeerMap          peers_{};
};
} // namespace foskv::raft