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
        co_await kosio::time::sleep(1000);
        co_await client.Put("shit", "ass");
        co_await kosio::time::sleep(1000);
        co_await client.Get("shit");
        co_await kosio::time::sleep(1000);
        co_await client.Delete("shit");
        co_await kosio::time::sleep(1000);
        co_await client.Get("shit");
        co_await kosio::time::sleep(1000);
        co_await client.Delete("shit");
    }
}

auto main_loop() -> kosio::async::Task<> {
    kosio::spawn(process());
    co_await kosio::signal::ctrl_c();
}

auto main() -> int {
    SET_LOG_LEVEL(kosio::log::LogLevel::Verbose);
    kosio::runtime::MultiThreadBuilder::default_create().block_on(main_loop());
}