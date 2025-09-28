#include "foskv/raft/raft_node.hpp"
#include <kosio/signal/signal.hpp>

using namespace foskv::raft;

auto process(std::unique_ptr<RaftNode>& node) -> kosio::async::Task<> {
    auto ret = co_await node->run();
    if (!ret) {
        LOG_ERROR("{}", ret.error());
    }
}

auto main_loop() -> kosio::async::Task<> {
    std::unordered_set<NodeInfo> nodes;
    nodes.emplace("node1", "127.0.0.1", 8080);
    nodes.emplace("node2", "127.0.0.1", 8081);
    nodes.emplace("node3", "127.0.0.1", 8082);
    // Node1
    auto has_save = co_await RaftConfig::save("node3/config.json", 0, "node3", nodes);
    if (!has_save) {
        LOG_ERROR("{}", has_save.error());
        co_return;
    }
    auto has_raft_node = co_await RaftNode::create("node3/config.json", "node3");
    if (!has_raft_node) {
        LOG_ERROR("{}", has_raft_node.error());
        co_return;
    }
    auto node = std::move(has_raft_node.value());
    kosio::spawn(process(node));
    co_await kosio::signal::ctrl_c();
    // Exit
    co_await node->shutdown();
}

auto main() -> int {
    SET_LOG_LEVEL(kosio::log::LogLevel::Verbose);
    kosio::runtime::CurrentThreadBuilder::default_create().block_on(main_loop());
}