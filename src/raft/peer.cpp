#include "foskv/raft/peer.hpp"

foskv::raft::Peer::Peer(const std::string &host, uint16_t port)
    : host_(host)
    , port_(port) {}

foskv::raft::Peer::Peer(Peer &&other) noexcept
    : host_(std::move(other.host_))
    , port_(other.port_)
    , consumer_(std::move(other.consumer_)) {}

foskv::raft::Peer & foskv::raft::Peer::operator=(Peer &&other) noexcept {
    host_ = std::move(other.host_);
    port_ = other.port_;
    consumer_ = std::move(other.consumer_);
    return *this;
}

auto foskv::raft::Peer::connect() -> kosio::async::Task<RpcResult<void>> {
    auto ret = co_await rpc::RpcConsumer::connect(host_, port_);
    if (!ret) {
        co_return std::unexpected{ret.error()};
    }
    consumer_ = std::move(ret.value());
    co_return RpcResult<void>{};
}
