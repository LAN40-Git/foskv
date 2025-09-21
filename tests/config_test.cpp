#include "foskv/raft/config.hpp"

constexpr std::string_view config_path = "config.json";

auto save() -> kosio::async::Task<foskv::Result<void>> {
    std::unordered_set<foskv::raft::NodeInfo> nodes;
    nodes.emplace("node1", "127.0.0.1", 8080);
    nodes.emplace("node2", "127.0.0.1", 8081);
    nodes.emplace("node3", "127.0.0.1", 8082);
    auto has_save = co_await foskv::raft::Config::save(config_path, 0, "node1", nodes);
    if (!has_save) {
        co_return std::unexpected{has_save.error()};
    }
    co_return foskv::Result<void>{};
}

auto load() -> kosio::async::Task<foskv::Result<foskv::raft::Config>> {
    auto has_config = co_await foskv::raft::Config::load(config_path);
    if (!has_config) {
        co_return std::unexpected{has_config.error()};
    }
    co_return std::move(has_config.value());
}

auto main_loop() -> kosio::async::Task<void> {
    auto ret = co_await save();
    if (!ret) {
        LOG_ERROR("Failed to save config {} : {}", config_path, ret.error());
        co_return;
    }
    auto has_config = co_await load();
    if (!has_config) {
        LOG_ERROR("Failed to load {} : {}", config_path, has_config.error());
    }
    auto config = std::move(has_config.value());
    while (true) {
        co_await kosio::time::sleep(10000);
        auto ret = co_await config.add_peer(0, "TEST", "127.0.0.1", 8084);
    }
}

auto main() -> int {
    kosio::runtime::MultiThreadBuilder::default_create().block_on(main_loop());
}