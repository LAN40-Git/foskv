#pragma once
#include "foskv/raft/raft_node.hpp"

namespace foskv::server {
class FoskvServer {
private:
    explicit FoskvServer(raft::RaftNode&& raft_node);

public:
    FoskvServer(FoskvServer&& other) noexcept;
    auto operator=(FoskvServer&& other) noexcept -> FoskvServer&;

public:
    [[REMEMBER_CO_AWAIT]]
    static auto create(const std::filesystem::path& config_path, const std::filesystem::path& data_dir) -> kosio::async::Task<ServerResult<FoskvServer>>;

public:
    auto run() -> kosio::async::Task<void>;

private:
    raft::RaftNode raft_node_;
};
} // namespace foskv::server