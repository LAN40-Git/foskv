#pragma once
#include "foskv/raft/util.hpp"

namespace foskv::raft {
class Peer {
public:
    explicit Peer(const kosio::net::SocketAddr& addr);

    Peer(Peer&& other) noexcept;
    Peer& operator=(Peer&& other) noexcept;

public:
    // raft rpc
    auto request_vote(std::string_view req_payload, rpc::RpcCallback&& callback) -> kosio::async::Task<>;
    auto append_entries(std::string_view req_payload, rpc::RpcCallback&& callback) -> kosio::async::Task<>;
    auto install_snapshot(std::string_view req_payload, rpc::RpcCallback&& callback) -> kosio::async::Task<>;

private:
    [[REMEMBER_CO_AWAIT]]
    auto connect() -> kosio::async::Task<kosio::Result<void>>;

private:
    kosio::net::SocketAddr            addr_;
    std::unique_ptr<rpc::RpcConsumer> consumer_{nullptr};
};
} // namespace foskv::raft