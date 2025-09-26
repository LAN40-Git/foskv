#include "foskv/rpc.hpp"
#include <kosio/signal.hpp>
using namespace foskv::rpc;

std::atomic<uint64_t> counter{1};

auto kv_put(std::unique_ptr<RpcConsumer>& consumer) -> kosio::async::Task<> {
    // kosio::log::console.info("{}", counter.fetch_add(1, std::memory_order_relaxed));
    foskv::kv::PutRequest put_request;
    put_request.set_key("key");
    put_request.set_value("value");
    using foskv::rpc::ServiceType;
    using foskv::rpc::MethodType;
    co_await consumer->call(ServiceType::kKv, MethodType::kKvPut, put_request.SerializeAsString(),
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
    });
}

auto kv_get(std::unique_ptr<RpcConsumer>& consumer) -> kosio::async::Task<> {
    foskv::kv::GetRequest get_request;
    get_request.set_key("key");
    using foskv::rpc::ServiceType;
    using foskv::rpc::MethodType;
    co_await consumer->call(ServiceType::kKv, MethodType::kKvGet, get_request.SerializeAsString(),
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
    using foskv::rpc::ServiceType;
    using foskv::rpc::MethodType;
    co_await consumer->call(ServiceType::kKv, MethodType::kKvDelete, delete_request.SerializeAsString(),
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

auto process(std::unique_ptr<RpcConsumer> consumer) -> kosio::async::Task<> {
    while (true) {
        for (int i = 0; i < 1000; i++) {
            kosio::spawn(kv_put_1000(consumer));
        }
        co_await kosio::time::sleep(30000);
    }
}

auto main_loop() -> kosio::async::Task<> {
    constexpr std::size_t CONSUMER_SIZE = 4;
    for (int i = 0; i < CONSUMER_SIZE; i++) {
        auto has_consumer = co_await RpcConsumer::create("127.0.0.1", 8080);
        if (!has_consumer) {
            co_return;
        }
        kosio::spawn(process(std::move(has_consumer.value())));
    }
    co_await kosio::signal::ctrl_c();
}

auto main() -> int {
    SET_LOG_LEVEL(kosio::log::LogLevel::Verbose);
    kosio::runtime::MultiThreadBuilder::options().set_num_workers(16).build().block_on(main_loop());
    // kosio::runtime::CurrentThreadBuilder::default_create().block_on(main_loop());
}