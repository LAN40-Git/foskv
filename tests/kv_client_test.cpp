#include "foskv/client/kv_client.hpp"
#include <kosio/signal/signal.hpp>
#include <nlohmann/detail/input/parser.hpp>

auto process() -> kosio::async::Task<> {
    auto has_kv_client = co_await foskv::client::KVClient::Connect("127.0.0.1", 8080);
    if (!has_kv_client) {
        LOG_ERROR("{}", has_kv_client.error());
        co_return;
    }
    auto client = std::move(has_kv_client.value());
    while (true) {
        for (int i = 0; i < 1000000; i++) {
            kosio::spawn(client.Put("SHIT", "ASS"));
        }
        co_await kosio::time::sleep(30000);
    }
}

auto main_loop() -> kosio::async::Task<> {
    for (int i = 0; i < 1; i++) {
        kosio::spawn(process());
    }
    co_await kosio::signal::ctrl_c();
}

auto main() -> int {
    SET_LOG_LEVEL(kosio::log::LogLevel::Verbose);
    kosio::runtime::MultiThreadBuilder::options().set_num_workers(16).build().block_on(main_loop());
}