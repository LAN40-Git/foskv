#include "foskv/raft/peer.hpp"

foskv::raft::Peer::Peer(const std::string &host, uint16_t port)
    : host_(host)
    , port_(port) {}

foskv::raft::Peer::Peer(Peer &&other) noexcept
    : host_(std::move(other.host_))
    , port_(other.port_)
    , consumer_(std::move(other.consumer_)) {}

auto foskv::raft::Peer::operator=(Peer &&other) noexcept -> Peer& {
    host_ = std::move(other.host_);
    port_ = other.port_;
    consumer_ = std::move(other.consumer_);
    return *this;
}

auto foskv::raft::Peer::request_vote(std::string_view payload, rpc::RpcCallback &&callback)
-> kosio::async::Task<> {
    constexpr std::string_view METHOD_NAME = "RequestVote";

    if (consumer_ == nullptr) [[unlikely]] {
        auto ret = co_await connect();
        if (!ret) [[unlikely]] {
            LOG_ERROR("Failed to connect to {}:{} : {}", host_, port_, ret.error());
        }
    }

    auto ret = co_await consumer_->call(SERVICE_NAME, METHOD_NAME, payload, std::move(callback));
    if (!ret) [[unlikely]] {
        LOG_ERROR("Failed to call rpc {}-{} : {}", SERVICE_NAME, METHOD_NAME, ret.error());
    }
}

auto foskv::raft::Peer::append_entries(std::string_view payload, rpc::RpcCallback &&callback)
-> kosio::async::Task<> {
    constexpr std::string_view METHOD_NAME = "AppendEntries";

    if (consumer_ == nullptr) [[unlikely]] {
        auto ret = co_await connect();
        if (!ret) [[unlikely]] {
            LOG_ERROR("Failed to connect to {}:{} : {}", host_, port_, ret.error());
        }
    }

    auto ret = co_await consumer_->call(SERVICE_NAME, METHOD_NAME, payload, std::move(callback));
    if (!ret) [[unlikely]] {
        LOG_ERROR("Failed to call rpc {}-{} : {}", SERVICE_NAME, METHOD_NAME, ret.error());
    }
}

auto foskv::raft::Peer::install_snapshot(std::string_view payload, rpc::RpcCallback &&callback)
-> kosio::async::Task<> {
    constexpr std::string_view METHOD_NAME = "InstallSnapshot";
    if (consumer_ == nullptr) [[unlikely]] {
        auto ret = co_await connect();
        if (!ret) [[unlikely]] {
            LOG_ERROR("Failed to connect to {}:{} : {}", host_, port_, ret.error());
        }
    }

    auto ret = co_await consumer_->call(SERVICE_NAME, METHOD_NAME, payload, std::move(callback));
    if (!ret) [[unlikely]] {
        LOG_ERROR("Failed to call rpc {}-{} : {}", SERVICE_NAME, METHOD_NAME, ret.error());
    }
}

auto foskv::raft::Peer::connect() -> kosio::async::Task<kosio::Result<void>> {
    auto ret = co_await rpc::RpcConsumer::connect(host_, port_);
    if (!ret) [[unlikely]] {
        co_return std::unexpected{ret.error()};
    }
    consumer_ = std::move(ret.value());
    co_return kosio::Result<void>{};
}
