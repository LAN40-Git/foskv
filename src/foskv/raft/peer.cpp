#include "foskv/raft/peer.hpp"

foskv::raft::detail::Peer::Peer(
    uint64_t member_id,
    std::string_view name,
    const kosio::net::SocketAddr& server_addr)
    : member_id_(member_id)
    , name_(name)
    , server_addr_(server_addr)
    , consumer_(server_addr) {}

auto foskv::raft::detail::Peer::request_vote(std::string_view req_payload, RpcCallback &&callback)
-> kosio::async::Task<> {
    auto ret = co_await consumer_.call(
        rpc::RaftService::ServiceName, rpc::RaftService::RequestVote, req_payload, std::move(callback));
    if (!ret) [[unlikely]] {
        LOG_ERROR("Failed to call rpc {}-{} : {}", rpc::RaftService::ServiceName, rpc::RaftService::RequestVote, ret.error());
    }
}

auto foskv::raft::detail::Peer::append_entries(std::string_view req_payload, RpcCallback &&callback)
-> kosio::async::Task<> {
    auto ret = co_await consumer_.call(
        rpc::RaftService::ServiceName, rpc::RaftService::AppendEntries, req_payload, std::move(callback));
    if (!ret) [[unlikely]] {
        LOG_ERROR("Failed to call rpc {}-{} : {}", rpc::RaftService::ServiceName, rpc::RaftService::AppendEntries, ret.error());
    }
}

auto foskv::raft::detail::Peer::install_snapshot(std::string_view req_payload, RpcCallback &&callback)
-> kosio::async::Task<> {
    auto ret = co_await consumer_.call(
        rpc::RaftService::ServiceName, rpc::RaftService::InstallSnapshot, req_payload, std::move(callback));
    if (!ret) [[unlikely]] {
        LOG_ERROR("Failed to call rpc {}-{} : {}", rpc::RaftService::ServiceName, rpc::RaftService::InstallSnapshot, ret.error());
    }
}

auto foskv::raft::detail::Peer::request_vote(std::string &&req_payload,
    RpcCallback &&callback) -> kosio::async::Task<> {
    auto ret = co_await consumer_.call(
        rpc::RaftService::ServiceName, rpc::RaftService::RequestVote, std::move(req_payload), std::move(callback));
    if (!ret) [[unlikely]] {
        LOG_ERROR("Failed to call rpc {}-{} : {}", rpc::RaftService::ServiceName, rpc::RaftService::RequestVote, ret.error());
    }
}

auto foskv::raft::detail::Peer::append_entries(std::string &&req_payload,
    RpcCallback &&callback) -> kosio::async::Task<> {
    auto ret = co_await consumer_.call(
        rpc::RaftService::ServiceName, rpc::RaftService::AppendEntries, std::move(req_payload), std::move(callback));
    if (!ret) [[unlikely]] {
        LOG_ERROR("Failed to call rpc {}-{} : {}", rpc::RaftService::ServiceName, rpc::RaftService::AppendEntries, ret.error());
    }
}

auto foskv::raft::detail::Peer::install_snapshot(std::string &&req_payload,
    RpcCallback &&callback) -> kosio::async::Task<> {
    auto ret = co_await consumer_.call(
        rpc::RaftService::ServiceName, rpc::RaftService::InstallSnapshot, std::move(req_payload), std::move(callback));
    if (!ret) [[unlikely]] {
        LOG_ERROR("Failed to call rpc {}-{} : {}", rpc::RaftService::ServiceName, rpc::RaftService::InstallSnapshot, ret.error());
    }
}
