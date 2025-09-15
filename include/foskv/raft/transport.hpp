#pragma once
#include <kosio/sync.hpp>
#include "foskv/raft/peer.hpp"

namespace foskv::raft {
class RaftTransport {
public:

private:
    std::unordered_map<uint64_t, Peer> peers_;
    kosio::sync::Mutex                 mutex_;
};
} // namespace foskv::raft