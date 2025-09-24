#include "foskv/rpc.hpp"
using namespace foskv::rpc;

auto main_loop() -> kosio::async::Task<> {
    auto has_consumer = co_await RpcConsumer::create("127.0.0.1", 8080);
    if (!has_consumer) {
        co_return;
    }
    auto consumer = std::move(has_consumer.value());
    foskv::kv::PutRequest put_request;
    put_request.set_key("key");
    put_request.set_value("value");
    foskv::kv::GetRequest get_request;
    get_request.set_key("key");
    foskv::kv::DeleteRequest delete_request;
    delete_request.set_key("key");
    while (true) {
        co_await kosio::time::sleep(3000);
        co_await consumer->call(KVService::ServiceName, KVService::Put, put_request.SerializeAsString(),
            [](std::string_view resp_payload) -> kosio::async::Task<> {
                foskv::kv::PutResponse response;
                if (!response.ParseFromArray(resp_payload.data(), resp_payload.size())) {
                    LOG_ERROR("Failed to parse put response");
                    co_return;
                }

                if (response.header().success()) {
                    LOG_INFO("Put succesful");
                } else {
                    LOG_ERROR("{}", RpcError{static_cast<int>(response.header().error_code())}.message());
                }
            });
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
}

auto main() -> int {
    SET_LOG_LEVEL(kosio::log::LogLevel::Verbose);
    kosio::runtime::MultiThreadBuilder::default_create().block_on(main_loop());
}