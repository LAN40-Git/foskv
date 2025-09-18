#include "foskv/raft/peer.hpp"

foskv::raft::detail::Peer::Peer(
    uint64_t member_id,
    std::string_view name,
    const kosio::net::SocketAddr& addr)
    : member_id_(member_id), name_(name), addr_(addr) {}

foskv::raft::detail::Peer::Peer(Peer &&other) noexcept
    : member_id_(other.member_id_)
    , addr_(other.addr_)
    , consumer_(std::move(other.consumer_)) {
    other.member_id_ = 0;
    other.addr_ = kosio::net::SocketAddr{};
}

auto foskv::raft::detail::Peer::operator=(Peer &&other) noexcept -> Peer& {
    member_id_ = other.member_id_;
    addr_ = other.addr_;
    consumer_ = std::move(other.consumer_);
    other.member_id_ = 0;
    other.addr_ = kosio::net::SocketAddr{};
    return *this;
}

auto foskv::raft::detail::Peer::create(uint64_t member_id, std::string_view name,
    std::string_view host, uint16_t port) -> RaftResult<Peer> {
    auto has_addr = kosio::net::SocketAddr::parse(host, port);
    if (!has_addr) {
        return std::unexpected{make_raft_error(RaftError::kInvalidPeerAddress)};
    }
    return Peer{member_id, name, has_addr.value()};
}

auto foskv::raft::detail::Peer::request_vote(std::string_view req_payload, rpc::RpcCallback &&callback)
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

auto foskv::raft::detail::Peer::append_entries(std::string_view req_payload, rpc::RpcCallback &&callback)
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

auto foskv::raft::detail::Peer::install_snapshot(std::string_view req_payload, rpc::RpcCallback &&callback)
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

auto foskv::raft::detail::Peer::connect() -> kosio::async::Task<kosio::Result<void>> {
    auto ret = co_await rpc::RpcConsumer::connect(addr_);
    if (!ret) [[unlikely]] {
        co_return std::unexpected{ret.error()};
    }
    consumer_ = std::move(ret.value());
    co_return kosio::Result<void>{};
}
