#include "foskv/raft/raft_node.hpp"
#include "kosio/signal.hpp"

auto server() -> kosio::async::Task<> {
    std::filesystem::path config_path = "./config.json";
    std::filesystem::path data_dir = "./data";
    auto has_raft_node = co_await foskv::raft::RaftNode::create(config_path, data_dir);
    if (!has_raft_node) {
        LOG_ERROR("Failed to create raft node : {}", has_raft_node.error());
        co_return;
    }
    try {
        auto raft_node = std::move(has_raft_node.value());
        co_await raft_node.run();
    } catch (...) {
        throw;
    }
}

auto main() -> int {
    kosio::runtime::CurrentThreadBuilder::default_create().block_on(server());
}