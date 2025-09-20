#include "foskv/raft/config.hpp"

auto main_loop() -> kosio::async::Task<> {
    std::string path = "./config.json";
    std::unordered_set<foskv::raft::NodeInfo> nodes;
    nodes.emplace("node1", "127.0.0.1", 8080);
    nodes.emplace("node2", "127.0.0.1", 8081);
    nodes.emplace("node3", "127.0.0.1", 8082);
    auto has_config = co_await foskv::raft::Config::save(path, 0, "node1", nodes);
    if (!has_config) {
        LOG_ERROR("Failed to create config file : {}", has_config.error());
    }
}

auto main() -> int {
    kosio::runtime::MultiThreadBuilder::default_create().block_on(main_loop());
}