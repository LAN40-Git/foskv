#include "foskv/client/kv_client.hpp"
#include <kosio/signal/signal.hpp>
#include <nlohmann/json.hpp>
#include <atomic>
#include <string>


std::atomic<int> g_success_count = 0;
std::atomic<int> g_fail_count = 0;

std::pair<std::string, std::string> generate_kv(int counter, std::size_t client_id) {
    std::string key = "client_" + std::to_string(client_id) + "_key_" + std::to_string(counter);
    std::string value = "value_" + std::to_string(client_id) + "_" + std::to_string(counter);
    return {key, value};
}

auto process(
    std::unique_ptr<foskv::client::KVClient>& client,
    std::size_t client_id
) -> kosio::async::Task<> {
    int counter = 0;
    while (true) {
        co_await kosio::time::sleep(10);
        for (int i = 0; i < 10; i++) {
            auto [key, value] = generate_kv(counter++, client_id);
            co_await client->Put(key, value);
        }
    }
}

auto main_loop() -> kosio::async::Task<> {
    constexpr std::size_t CLIENT_SIZE = 10;
    std::array<std::unique_ptr<foskv::client::KVClient>, CLIENT_SIZE> clients{};

    for (std::size_t i = 0; i < CLIENT_SIZE; i++) {
        auto has_kv_client = co_await foskv::client::KVClient::Connect("127.0.0.1", 8082);
        if (!has_kv_client) {
            LOG_ERROR("Client {} connect failed: {}", i, has_kv_client.error());
            co_return;
        }
        clients[i] = std::move(has_kv_client.value());
        LOG_INFO("Client {} connected", i);
    }

    for (std::size_t i = 0; i < CLIENT_SIZE; i++) {
        kosio::spawn(process(clients[i], i));
    }

    co_await kosio::signal::ctrl_c();
    LOG_INFO("Test stopped. Success: {}, Fail: {}, Total: {}",
             g_success_count.load(),
             g_fail_count.load(),
             g_success_count + g_fail_count);
}

auto main() -> int {
    SET_LOG_LEVEL(kosio::log::LogLevel::Verbose);
    kosio::runtime::MultiThreadBuilder::options().set_num_workers(4).build().block_on(main_loop());
    return 0;
}