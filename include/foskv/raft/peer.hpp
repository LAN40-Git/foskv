#pragma once
#include "foskv/rpc/rpc.hpp"
#include "foskv/storage/storage.hpp"

namespace foskv::raft::detail {
class Peer {
    using RpcCallback = rpc::detail::RpcCallback;
    friend class Transport;
public:
    explicit Peer(uint64_t member_id, std::string_view name, const kosio::net::SocketAddr& server_addr);

public:
    auto member_id() const noexcept -> uint64_t { return member_id_; }
    auto name() const noexcept -> std::string { return name_; }

public:
    // raft rpc
    auto request_vote(std::string_view req_payload, RpcCallback&& callback) -> kosio::async::Task<>;
    auto append_entries(std::string_view req_payload, RpcCallback&& callback) -> kosio::async::Task<>;
    auto install_snapshot(std::string_view req_payload, RpcCallback&& callback) -> kosio::async::Task<>;
    auto request_vote(std::string&& req_payload, RpcCallback&& callback) -> kosio::async::Task<>;
    auto append_entries(std::string&& req_payload, RpcCallback&& callback) -> kosio::async::Task<>;
    auto install_snapshot(std::string&& req_payload, RpcCallback&& callback) -> kosio::async::Task<>;

private:
    uint64_t               member_id_;
    std::string            name_;
    kosio::net::SocketAddr server_addr_;
    rpc::RpcConsumer       consumer_;
};
} // namespace foskv::raft::detail