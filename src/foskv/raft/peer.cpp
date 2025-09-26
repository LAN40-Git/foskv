#include "foskv/raft/peer.hpp"

foskv::raft::detail::Peer::Peer(
    uint64_t member_id, std::string_view name,
    std::string_view host, uint16_t port,
    std::unique_ptr<rpc::RpcConsumer> consumer)
    : member_id_(member_id)
    , name_(name)
    , host_(host)
    , port_(port)
    , consumer_(std::move(consumer)) {}

auto foskv::raft::detail::Peer::create(
    uint64_t member_id,
    std::string_view name,
    std::string_view host,
    uint16_t port) -> kosio::async::Task<Result<Peer>> {
    auto has_consumer = co_await rpc::RpcConsumer::create(host, port);
    if (!has_consumer) {
        co_return std::unexpected{has_consumer.error()};
    }
    co_return Peer{member_id, name, host, port, std::move(has_consumer.value())};
}

auto foskv::raft::detail::Peer::shutdown() const -> kosio::async::Task<> {
    co_await consumer_->shutdown();
}

auto foskv::raft::detail::Peer::request_vote(std::string_view req_payload, RpcCallback &&callback)
const -> kosio::async::Task<> {
    using rpc::ServiceType;
    using rpc::MethodType;
    auto ret = co_await consumer_->call(
        ServiceType::kRaft, MethodType::kRaftRequestVote, req_payload, std::move(callback));
    if (!ret) [[unlikely]] {
        LOG_ERROR("Failed to call rpc `RequestVote`");
    }
}

auto foskv::raft::detail::Peer::append_entries(std::string_view req_payload, RpcCallback &&callback)
const -> kosio::async::Task<> {
    using rpc::ServiceType;
    using rpc::MethodType;
    auto ret = co_await consumer_->call(
        ServiceType::kRaft, MethodType::kRaftAppendEntries, req_payload, std::move(callback));
    if (!ret) [[unlikely]] {
        LOG_ERROR("Failed to call rpc `AppendEntries`");
    }
}

auto foskv::raft::detail::Peer::install_snapshot(std::string_view req_payload, RpcCallback &&callback)
const -> kosio::async::Task<> {
    using rpc::ServiceType;
    using rpc::MethodType;
    auto ret = co_await consumer_->call(
        ServiceType::kRaft, MethodType::kRaftInstallSnapshot, req_payload, std::move(callback));
    if (!ret) [[unlikely]] {
        LOG_ERROR("Failed to call rpc `InstallSnapshot`");
    }
}

auto foskv::raft::detail::Peer::request_vote(std::string &&req_payload, RpcCallback &&callback)
const -> kosio::async::Task<> {
    using rpc::ServiceType;
    using rpc::MethodType;
    auto ret = co_await consumer_->call(
        ServiceType::kRaft, MethodType::kRaftRequestVote, std::move(req_payload), std::move(callback));
    if (!ret) [[unlikely]] {
        LOG_ERROR("Failed to call rpc `RequestVote`");
    }
}

auto foskv::raft::detail::Peer::append_entries(std::string &&req_payload, RpcCallback &&callback)
const -> kosio::async::Task<> {
    using rpc::ServiceType;
    using rpc::MethodType;
    auto ret = co_await consumer_->call(
        ServiceType::kRaft, MethodType::kRaftAppendEntries, std::move(req_payload), std::move(callback));
    if (!ret) [[unlikely]] {
        LOG_ERROR("Failed to call rpc `AppendEntries`");
    }
}

auto foskv::raft::detail::Peer::install_snapshot(std::string &&req_payload, RpcCallback &&callback)
const -> kosio::async::Task<> {
    using rpc::ServiceType;
    using rpc::MethodType;
    auto ret = co_await consumer_->call(
        ServiceType::kRaft, MethodType::kRaftInstallSnapshot, std::move(req_payload), std::move(callback));
    if (!ret) [[unlikely]] {
        LOG_ERROR("Failed to call rpc `InstallSnapshot`");
    }
}