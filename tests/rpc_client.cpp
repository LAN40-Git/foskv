#include "foskv/rpc/consumer.hpp"
#include "foskv/storage/command.hpp"
#include "foskv/storage/storage.hpp"
#include <kosio/signal/signal.hpp>

constexpr std::string_view SERVICE_NAME = "KVStorage";

auto RpcPut(foskv::rpc::RpcConsumer* consumer, std::string_view payload, foskv::rpc::RpcCallback&& callback)
-> kosio::async::Task<> {
    constexpr std::string_view METHOD_NAME = "Put";
    auto ret = co_await consumer->call(SERVICE_NAME, METHOD_NAME, payload, std::move(callback));
    if (!ret) [[unlikely]] {
        kosio::log::console.error("Failed to call rpc {}-{} : {}", SERVICE_NAME, METHOD_NAME, ret.error());
    }
}

auto RpcPutCallback(foskv::RpcResult<std::string_view> has_response) -> kosio::async::Task<> {
    if (!has_response) {
        kosio::log::console.error("{}", has_response.error());
        co_return;
    }
    foskv::storage::PutResponse put_response;
    if (!put_response.ParseFromArray(has_response.value().data(), has_response.value().size())) {
        kosio::log::console.error("Failed to parse response");
        co_return;
    }
    if (put_response.header().success()) {
        kosio::log::console.info("Success");
    } else {
        kosio::log::console.error("{}", put_response.header().error());
    }
}

auto RpcGet(foskv::rpc::RpcConsumer* consumer, std::string_view payload, foskv::rpc::RpcCallback&& callback)
-> kosio::async::Task<> {
    constexpr std::string_view METHOD_NAME = "Get";
    auto ret = co_await consumer->call(SERVICE_NAME, METHOD_NAME, payload, std::move(callback));
    if (!ret) [[unlikely]] {
        kosio::log::console.error("Failed to call rpc {}-{} : {}", SERVICE_NAME, METHOD_NAME, ret.error());
    }
}

auto RpcGetCallback(foskv::RpcResult<std::string_view> has_response) -> kosio::async::Task<> {
    if (!has_response) {
        kosio::log::console.error("{}", has_response.error());
        co_return;
    }
    foskv::storage::GetResponse get_response;
    if (!get_response.ParseFromArray(has_response.value().data(), has_response.value().size())) {
        kosio::log::console.error("Failed to parse response");
        co_return;
    }
    if (get_response.header().success()) {
        kosio::log::console.info("{}", get_response.value());
    } else {
        kosio::log::console.error("{}", get_response.header().error());
    }
}

auto RpcDelete(foskv::rpc::RpcConsumer* consumer, std::string_view payload, foskv::rpc::RpcCallback&& callback)
-> kosio::async::Task<> {
    constexpr std::string_view METHOD_NAME = "Delete";
    auto ret = co_await consumer->call(SERVICE_NAME, METHOD_NAME, payload, std::move(callback));
    if (!ret) [[unlikely]] {
        kosio::log::console.error("Failed to call rpc {}-{} : {}", SERVICE_NAME, METHOD_NAME, ret.error());
    }
}

auto RpcDeleteCallback(foskv::RpcResult<std::string_view> has_response) -> kosio::async::Task<> {
    if (!has_response) {
        kosio::log::console.error("{}", has_response.error());
        co_return;
    }
    foskv::storage::DeleteResponse delete_response;
    if (!delete_response.ParseFromArray(has_response.value().data(), has_response.value().size())) {
        kosio::log::console.error("Failed to parse response");
        co_return;
    }
    if (delete_response.header().success()) {
        kosio::log::console.info("Success");
    } else {
        kosio::log::console.error("{}", delete_response.header().error());
    }
}

auto process(std::unique_ptr<foskv::rpc::RpcConsumer> consumer) -> kosio::async::Task<> {
    foskv::storage::PutRequest put_request;
    foskv::storage::GetRequest get_request;
    foskv::storage::DeleteRequest delete_request;
    while (true) {
        auto args = co_await foskv::storage::KVCommand::async_parse();
        switch (args.op) {
            case foskv::storage::KVCommand::Op::kPut: {
                put_request.set_key(args.key);
                put_request.set_value(args.value);
                auto payload = put_request.SerializeAsString();
                co_await consumer->call(SERVICE_NAME, "Put", payload, [](foskv::RpcResult<std::string_view> has_response) -> kosio::async::Task<> {
                    if (!has_response) {
                        kosio::log::console.error("{}", has_response.error());
                        co_return;
                    }
                    foskv::storage::PutResponse put_response;
                    if (!put_response.ParseFromArray(has_response.value().data(), has_response.value().size())) {
                        kosio::log::console.error("Failed to parse response");
                        co_return;
                    }
                    if (put_response.header().success()) {
                        kosio::log::console.info("Success");
                    } else {
                        kosio::log::console.error("{}", put_response.header().error());
                    }
                });
                // kosio::spawn(RpcPut(consumer.get(), payload, RpcPutCallback));
                break;
            }
            case foskv::storage::KVCommand::Op::kGet: {
                get_request.set_key(args.key);
                auto payload = get_request.SerializeAsString();
                kosio::spawn(RpcGet(consumer.get(), payload, RpcGetCallback));
                break;
            }
            case foskv::storage::KVCommand::Op::kDelete: {
                delete_request.set_key(args.key);
                auto payload = delete_request.SerializeAsString();
                kosio::spawn(RpcDelete(consumer.get(), payload, RpcDeleteCallback));
                break;
            }
            default: {
                kosio::log::console.error("Unknown kv command");
                break;
            }
        }
    }
}

auto main_loop() -> kosio::async::Task<> {
    auto has_consumer = co_await foskv::rpc::RpcConsumer::connect("127.0.0.1", 8080);
    if (!has_consumer) {
        kosio::log::console.error("{}", has_consumer.error());
        co_return;
    }
    kosio::spawn(process(std::move(has_consumer.value())));
    co_await kosio::signal::ctrl_c();
}

auto main() -> int {
    kosio::runtime::MultiThreadBuilder::default_create().block_on(main_loop());
}