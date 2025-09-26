#include "foskv/raft/raft_node.hpp"

#include <kosio/signal/signal.hpp>

using namespace foskv::raft;

auto node1() -> kosio::async::Task<> {
    std::unordered_set<NodeInfo> nodes;
    nodes.emplace("node1", "127.0.0.1", 8080);
    nodes.emplace("node2", "127.0.0.1", 8081);
    nodes.emplace("node3", "127.0.0.1", 8082);
    auto has_save = co_await RaftConfig::save("node1/config.json", 0, "node1", nodes);
    if (!has_save) {
        LOG_ERROR("{}", has_save.error());
        co_return;
    }

    auto has_raft_node = co_await RaftNode::create("node1/config.json", "node1");
    if (!has_raft_node) {
        LOG_ERROR("{}", has_raft_node.error());
        co_return;
    }
    auto raft_node = std::move(has_raft_node.value());
    LOG_INFO("Successfully create raft node");
    co_await raft_node->run();
}

auto node2() -> kosio::async::Task<> {
    std::unordered_set<NodeInfo> nodes;
    nodes.emplace("node1", "127.0.0.1", 8080);
    nodes.emplace("node2", "127.0.0.1", 8081);
    nodes.emplace("node3", "127.0.0.1", 8082);
    auto has_save = co_await RaftConfig::save("node2/config.json", 0, "node2", nodes);
    if (!has_save) {
        LOG_ERROR("{}", has_save.error());
        co_return;
    }

    auto has_raft_node = co_await RaftNode::create("node2/config.json", "node2");
    if (!has_raft_node) {
        LOG_ERROR("{}", has_raft_node.error());
        co_return;
    }
    auto raft_node = std::move(has_raft_node.value());
    LOG_INFO("Successfully create raft node");
    co_await raft_node->run();
}

auto node3() -> kosio::async::Task<> {
    std::unordered_set<NodeInfo> nodes;
    nodes.emplace("node1", "127.0.0.1", 8080);
    nodes.emplace("node2", "127.0.0.1", 8081);
    nodes.emplace("node3", "127.0.0.1", 8082);
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
    auto raft_node = std::move(has_raft_node.value());
    co_await raft_node->run();
}

auto main_loop() -> kosio::async::Task<> {
    kosio::spawn(node1());
    kosio::spawn(node2());
    kosio::spawn(node3());
    co_await kosio::signal::ctrl_c();
}

auto main() -> int {
    SET_LOG_LEVEL(kosio::log::LogLevel::Verbose);
    kosio::runtime::CurrentThreadBuilder::default_create().block_on(main_loop());
}