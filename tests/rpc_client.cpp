#include "foskv/rpc/consumer.hpp"
#include "foskv/storage/command.hpp"
#include "foskv/storage/storage.hpp"
#include <kosio/signal/signal.hpp>

auto process(foskv::rpc::RpcConsumer& consumer) -> kosio::async::Task<> {
    while (true) {
        auto args = co_await foskv::storage::KVCommand::async_parse();
        switch (args.op) {
            case foskv::storage::KVCommand::Op::kPut: {
                foskv::storage::PutRequest request;
                request.set_key(args.key);
                request.set_value(args.value);
                kosio::spawn(consumer.call<foskv::storage::PutResponse>(
                    "KVStorage", "Put", request.SerializeAsString(),
                    [](foskv::RpcResult<foskv::storage::PutResponse> has_response) -> kosio::async::Task<> {
                    if (!has_response) {
                        kosio::log::console.error("{}", has_response.error());
                    } else {
                        const auto& response = has_response.value();
                        if (response.header().success()) {
                            kosio::log::console.info("Success");
                        } else {
                            kosio::log::console.error("{}", response.header().error());
                        }
                    }
                    co_return;
                }));
                break;
            }
            case foskv::storage::KVCommand::Op::kGet: {
                foskv::storage::GetRequest request;
                request.set_key(args.key);
                kosio::spawn(consumer.call<foskv::storage::GetResponse>(
                    "KVStorage", "Get", request.SerializeAsString(),
                    [](foskv::RpcResult<foskv::storage::GetResponse> has_response) -> kosio::async::Task<> {
                    if (!has_response) {
                        kosio::log::console.error("{}", has_response.error());
                    } else {
                        const auto& response = has_response.value();
                        if (response.header().success()) {
                            kosio::log::console.info("{}", response.value());
                        } else {
                            kosio::log::console.error("{}", response.header().error());
                        }
                    }
                    co_return;
                }));
                break;
            }
            case foskv::storage::KVCommand::Op::kDelete: {
                foskv::storage::GetRequest request;
                request.set_key(args.key);
                kosio::spawn(consumer.call<foskv::storage::DeleteResponse>(
                    "KVStorage", "Delete", request.SerializeAsString(),
                    [](foskv::RpcResult<foskv::storage::DeleteResponse> has_response) -> kosio::async::Task<> {
                    if (!has_response) {
                        kosio::log::console.error("{}", has_response.error());
                    } else {
                        const auto& response = has_response.value();
                        if (response.header().success()) {
                            kosio::log::console.info("Success");
                        } else {
                            kosio::log::console.error("{}", response.header().error());
                        }
                    }
                    co_return;
                }));
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
        kosio::log::console.error("Failed to connect to RPC server");
        co_return;
    }
    kosio::spawn(process(has_consumer.value()));
    co_await kosio::signal::ctrl_c();
}

auto main() -> int {
    kosio::runtime::MultiThreadBuilder::default_create().block_on(main_loop());
}