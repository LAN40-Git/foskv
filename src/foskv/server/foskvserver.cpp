#include "foskv/server/foskvserver.hpp"

foskv::server::FoskvServer::FoskvServer(raft::RaftNode&& raft_node)
    : raft_node_(std::move(raft_node)) {}

foskv::server::FoskvServer::FoskvServer(FoskvServer &&other) noexcept
    : raft_node_(std::move(other.raft_node_)) {}

auto foskv::server::FoskvServer::operator=(FoskvServer &&other) noexcept -> FoskvServer & {
    raft_node_ = std::move(other.raft_node_);
    return *this;
}

auto foskv::server::FoskvServer::create(
    const std::filesystem::path &config_path,
    const std::filesystem::path &data_dir)
    -> kosio::async::Task<ServerResult<FoskvServer>> {
    auto has_raft_node = co_await raft::RaftNode::create(config_path, data_dir);
    if (!has_raft_node) {
        LOG_ERROR("{}", has_raft_node.error());
        co_return std::unexpected{make_server_error(ServerError::kRaftNodeCreationFailed)};
    }
    co_return FoskvServer{std::move(has_raft_node.value())};
}

auto foskv::server::FoskvServer::run() -> kosio::async::Task<void> {
    try {
        co_await raft_node_.run();
    } catch (...) {
        throw;
    }
}
