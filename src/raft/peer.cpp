#include "foskv/raft/peer.hpp"

foskv::raft::Peer::Peer(const std::string &host, uint16_t port, uint64_t member_id)
    : host_(host)
    , port_(port)
    , member_id_(member_id) {}

foskv::raft::Peer::Peer(Peer &&other) noexcept
    : host_(std::move(other.host_))
    , port_(other.port_)
    , member_id_(other.member_id_)
    , consumer_(std::move(other.consumer_)) {}

foskv::raft::Peer & foskv::raft::Peer::operator=(Peer &&other) noexcept {
    host_ = std::move(other.host_);
    port_ = other.port_;
    member_id_ = other.member_id_;
    consumer_ = std::move(other.consumer_);
    return *this;
}

auto foskv::raft::Peer::request_vote(const std::string& payload) -> kosio::async::Task<RequestVoteResponse> {
    if (!consumer_) {
        auto ret = co_await connect();
        if (!ret) {
            LOG_ERROR("{}", ret.error());

        }
    }
}

auto foskv::raft::Peer::append_entries(const std::string& payload) -> kosio::async::Task<AppendEntriesResponse> {

}

auto foskv::raft::Peer::connect() -> kosio::async::Task<RpcResult<void>> {
    auto ret = co_await rpc::RpcConsumer::connect(host_, port_);
    if (!ret) {
        co_return std::unexpected{ret.error()};
    }
    consumer_ = std::make_unique<rpc::RpcConsumer>(std::move(ret.value()));
    co_return RpcResult<void>{};
}
