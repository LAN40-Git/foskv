#include "foskv/server/foskvserver.hpp"

auto server() -> kosio::async::Task<> {
    std::string config_path = "./config.json";
    std::string data_dir = "./data";
    auto has_server = co_await foskv::server::FoskvServer::create(config_path, data_dir);
    if (!has_server) {
        LOG_ERROR("Failed to create server : {}", has_server.error());
        co_return;
    }
    try {
        co_await has_server.value().run();
    } catch (...) {
        throw;
    }
}

auto main() -> int {
    kosio::runtime::MultiThreadBuilder::default_create().block_on(server());
}