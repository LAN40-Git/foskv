#pragma once
#include "foskv/rpc/consumer.hpp"
#include "foskv/raft/raft.pb.h"

namespace foskv::raft {
class Peer {
public:
    explicit Peer(const std::string& host, uint16_t port, uint64_t member_id);
    Peer(Peer&& other) noexcept;
    Peer& operator=(Peer&& other) noexcept;

public:
    auto request_vote(const std::string& payload) -> kosio::async::Task<RequestVoteResponse>;
    auto append_entries(const std::string& payload) -> kosio::async::Task<AppendEntriesResponse>;

private:
    auto connect() -> kosio::async::Task<RpcResult<void>>;

private:
    std::string host_;
    uint16_t    port_;
    uint64_t    member_id_;
    std::unique_ptr<rpc::RpcConsumer> consumer_;
};
} // namespace foskv::raft