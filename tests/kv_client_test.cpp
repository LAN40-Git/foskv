#include "foskv/client/kv_client.hpp"
#include <kosio/signal/signal.hpp>
#include <nlohmann/detail/input/parser.hpp>

auto process(std::unique_ptr<foskv::client::KVClient>& client) -> kosio::async::Task<> {
    while (true) {
        co_await kosio::time::sleep(1);
        for (std::size_t i = 0; i < 7; i++) {
            co_await client->Put("SHIT", "ASS");
        }
    }
}

auto main_loop() -> kosio::async::Task<> {
    constexpr std::size_t CLIENT_SIZE = 1;
    std::array<std::unique_ptr<foskv::client::KVClient>, CLIENT_SIZE> clients{};
    for (std::size_t i = 0; i < CLIENT_SIZE; i++) {
        auto has_kv_client = co_await foskv::client::KVClient::Connect("127.0.0.1", 8080);
        if (!has_kv_client) {
            LOG_ERROR("{}", has_kv_client.error());
            co_return;
        }
        clients[i] = std::move(has_kv_client.value());
    }
    for (std::size_t i = 0; i < CLIENT_SIZE; i++) {
        kosio::spawn(process(clients[i]));
    }
    co_await kosio::signal::ctrl_c();
    for (std::size_t i = 0; i < CLIENT_SIZE; i++) {
        co_await clients[i]->Close();
    }
}

auto main() -> int {
    SET_LOG_LEVEL(kosio::log::LogLevel::Verbose);
    kosio::runtime::MultiThreadBuilder::options().set_num_workers(16).build().block_on(main_loop());
}