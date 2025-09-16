#include "foskv/rpc/consumer.hpp"
#include "foskv/storage/command.hpp"
#include "foskv/storage/storage.hpp"
#include <kosio/signal/signal.hpp>

auto process(std::unique_ptr<foskv::rpc::RpcConsumer> consumer) -> kosio::async::Task<> {
    while (true) {
        auto args = co_await foskv::storage::KVCommand::async_parse();
        switch (args.op) {
            case foskv::storage::KVCommand::Op::kPut: {
                foskv::storage::PutRequest request;
                request.set_key(args.key);
                request.set_value(args.value);
                co_await consumer->call("KVStorage", "Put", request.SerializeAsString(),
                [](foskv::RpcResult<std::string_view> has_response) -> kosio::async::Task<> {
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
                break;
            }
            case foskv::storage::KVCommand::Op::kGet: {
                foskv::storage::GetRequest request;
                request.set_key(args.key);
                co_await consumer->call("KVStorage", "Get", request.SerializeAsString(),
                [](foskv::RpcResult<std::string_view> has_response) -> kosio::async::Task<> {
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
                });
                break;
            }
            case foskv::storage::KVCommand::Op::kDelete: {
                foskv::storage::GetRequest request;
                request.set_key(args.key);
                co_await consumer->call("KVStorage", "Delete", request.SerializeAsString(),
                [](foskv::RpcResult<std::string_view> has_response) -> kosio::async::Task<> {
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
                });
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