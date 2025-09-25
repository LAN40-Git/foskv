#include "foskv/rpc.hpp"
#include <kosio/signal.hpp>
using namespace foskv::rpc;

std::chrono::steady_clock::time_point start;
std::chrono::steady_clock::time_point end;
std::atomic<int> counter{1};

auto kv_put(std::unique_ptr<RpcConsumer>& consumer) -> kosio::async::Task<> {
    foskv::kv::PutRequest put_request;
    std::string key = "key";
    std::string value = "value";
    put_request.mutable_key()->swap(key);
    put_request.mutable_value()->swap(value);
    co_await consumer->call(KVService::ServiceName, KVService::Put, put_request.SerializeAsString(),
    [](std::string_view resp_payload) -> kosio::async::Task<> {
        foskv::kv::PutResponse response;
        if (!response.ParseFromArray(resp_payload.data(), resp_payload.size())) {
            LOG_ERROR("Failed to parse put response");
            co_return;
        }

        if (response.header().success()) {
            // LOG_INFO("Put succesful");
        } else {
            LOG_ERROR("{}", RpcError{static_cast<int>(response.header().error_code())}.message());
        }
        if (auto ret = counter.fetch_add(1, std::memory_order_relaxed); ret % 10000 == 0) {
            end = std::chrono::steady_clock::now();
            auto duration_us = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

            kosio::log::console.info("1w kv_put rpc request, take {} ms, counter: {}",
                duration_us, ret);

            start = std::chrono::steady_clock::now();
        }
    });
}

auto kv_get(std::unique_ptr<RpcConsumer>& consumer) -> kosio::async::Task<> {
    foskv::kv::GetRequest get_request;
    get_request.set_key("key");
    co_await consumer->call(KVService::ServiceName, KVService::Get, get_request.SerializeAsString(),
    [](std::string_view resp_payload) -> kosio::async::Task<> {
        foskv::kv::GetResponse response;
        if (!response.ParseFromArray(resp_payload.data(), resp_payload.size())) {
            LOG_ERROR("Failed to parse get response");
            co_return;
        }

        if (response.header().success()) {
            LOG_INFO("Get succesful : {}-{}", response.kv().key(), response.kv().value());
        } else {
            LOG_ERROR("{}", RpcError{static_cast<int>(response.header().error_code())}.message());
        }
    });
}

auto kv_delete(std::unique_ptr<RpcConsumer>& consumer) -> kosio::async::Task<> {
    foskv::kv::DeleteRequest delete_request;
    delete_request.set_key("key");
    co_await consumer->call(KVService::ServiceName, KVService::Delete, delete_request.SerializeAsString(),
    [](std::string_view resp_payload) -> kosio::async::Task<> {
        foskv::kv::DeleteResponse response;
        if (!response.ParseFromArray(resp_payload.data(), resp_payload.size())) {
            LOG_ERROR("Failed to parse delete response");
            co_return;
        }

        if (response.header().success()) {
            LOG_INFO("Delete succesful");
        } else {
            LOG_ERROR("{}", RpcError{static_cast<int>(response.header().error_code())}.message());
        }
    });
}

auto kv_put_1000(std::unique_ptr<RpcConsumer>& consumer) -> kosio::async::Task<> {
    for (int i = 0; i < 1000; i++) {
        co_await kv_put(consumer);
    }
}

auto main_loop() -> kosio::async::Task<> {
    auto has_consumer = co_await RpcConsumer::create("127.0.0.1", 8080);
    if (!has_consumer) {
        co_return;
    }
    auto consumer = std::move(has_consumer.value());
    start = std::chrono::steady_clock::now();
    while (true) {
        for (int i = 0; i < 1000; i++) {
            kosio::spawn(kv_put_1000(consumer));
        }
        co_await kosio::time::sleep(100000);
    }
}

auto main() -> int {
    SET_LOG_LEVEL(kosio::log::LogLevel::Verbose);
    kosio::runtime::MultiThreadBuilder::default_create().block_on(main_loop());
}