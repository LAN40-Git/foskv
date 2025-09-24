#include "foskv/raft/raft_node.hpp"

using namespace foskv::raft;

constexpr std::string_view config_path = "config.json";

auto save() -> kosio::async::Task<foskv::Result<void>> {
    std::unordered_set<NodeInfo> nodes;
    nodes.emplace("node1", "127.0.0.1", 8080);
    nodes.emplace("node2", "127.0.0.1", 8081);
    nodes.emplace("node3", "127.0.0.1", 8082);
    auto has_save = co_await RaftConfig::save(config_path, 0, "node1", nodes);
    if (!has_save) {
        co_return std::unexpected{has_save.error()};
    }
    co_return foskv::Result<void>{};
}

auto main_loop() -> kosio::async::Task<> {
    if (auto ret = co_await save(); !ret) {
        LOG_ERROR("{}", ret.error());
        co_return;
    }

    auto has_raft_node = co_await RaftNode::create("config.json", "foskv");
    if (!has_raft_node) {
        LOG_ERROR("{}", has_raft_node.error());
        co_return;
    }
    auto raft_node = std::move(has_raft_node.value());
    LOG_INFO("Successfully create raft node");
    co_await raft_node->run();
}

auto main() -> int {
    SET_LOG_LEVEL(kosio::log::LogLevel::Verbose);
    kosio::runtime::CurrentThreadBuilder::default_create().block_on(main_loop());
}