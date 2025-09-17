#include "foskv/raft/peer.hpp"

foskv::raft::Peer::Peer(const kosio::net::SocketAddr& addr)
    : addr_(addr) {}

foskv::raft::Peer::Peer(Peer &&other) noexcept
    : addr_(other.addr_)
    , consumer_(std::move(other.consumer_)) {}

auto foskv::raft::Peer::operator=(Peer &&other) noexcept -> Peer& {
    addr_ = other.addr_;
    consumer_ = std::move(other.consumer_);
    return *this;
}

auto foskv::raft::Peer::request_vote(std::string_view req_payload, rpc::RpcCallback &&callback)
-> kosio::async::Task<> {
    if (consumer_ == nullptr) [[unlikely]] {
        auto ret = co_await connect();
        if (!ret) [[unlikely]] {
            LOG_ERROR("Failed to connect to {} : {}", addr_, ret.error());
            co_return;
        }
    }

    auto ret = co_await consumer_->call(
        RaftRpc::ServiceName, RaftRpc::RequestVote, req_payload, std::move(callback));
    if (!ret) [[unlikely]] {
        LOG_ERROR("Failed to call rpc {}-{} : {}", RaftRpc::ServiceName, RaftRpc::RequestVote, ret.error());
    }
}

auto foskv::raft::Peer::append_entries(std::string_view req_payload, rpc::RpcCallback &&callback)
-> kosio::async::Task<> {
    if (consumer_ == nullptr) [[unlikely]] {
        auto ret = co_await connect();
        if (!ret) [[unlikely]] {
            LOG_ERROR("Failed to connect to {} : {}", addr_, ret.error());
            co_return;
        }
    }

    auto ret = co_await consumer_->call(
        RaftRpc::ServiceName, RaftRpc::AppendEntries, req_payload, std::move(callback));
    if (!ret) [[unlikely]] {
        LOG_ERROR("Failed to call rpc {}-{} : {}", RaftRpc::ServiceName, RaftRpc::AppendEntries, ret.error());
    }
}

auto foskv::raft::Peer::install_snapshot(std::string_view req_payload, rpc::RpcCallback &&callback)
-> kosio::async::Task<> {
    constexpr std::string_view METHOD_NAME = "InstallSnapshot";
    if (consumer_ == nullptr) [[unlikely]] {
        auto ret = co_await connect();
        if (!ret) [[unlikely]] {
            LOG_ERROR("Failed to connect to {} : {}", addr_, ret.error());
            co_return;
        }
    }

    auto ret = co_await consumer_->call(
        RaftRpc::ServiceName, RaftRpc::InstallSnapshot, req_payload, std::move(callback));
    if (!ret) [[unlikely]] {
        LOG_ERROR("Failed to call rpc {}-{} : {}", RaftRpc::ServiceName, RaftRpc::InstallSnapshot, ret.error());
    }
}

auto foskv::raft::Peer::connect() -> kosio::async::Task<kosio::Result<void>> {
    auto ret = co_await rpc::RpcConsumer::connect(addr_);
    if (!ret) [[unlikely]] {
        co_return std::unexpected{ret.error()};
    }
    consumer_ = std::move(ret.value());
    co_return kosio::Result<void>{};
}
