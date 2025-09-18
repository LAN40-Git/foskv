#include "foskv/rpc/consumer.hpp"
#include "foskv/storage/command.hpp"
#include "foskv/storage/storage.hpp"
#include "foskv/raft/raft_node.hpp"
#include <kosio/signal/signal.hpp>

using namespace foskv::storage;

auto RpcPutCallback(std::string_view resp_payload) -> kosio::async::Task<> {
    PutResponse put_response;
    if (!put_response.ParseFromArray(resp_payload.data(), resp_payload.size())) {
        kosio::log::console.error("Failed to parse response");
        co_return;
    }
    if (put_response.header().success()) {
        kosio::log::console.info("Success");
    } else {
        kosio::log::console.error("{}", put_response.header().error());
    }
}

auto RpcGetCallback(std::string_view resp_payload) -> kosio::async::Task<> {
    GetResponse get_response;
    if (!get_response.ParseFromArray(resp_payload.data(), resp_payload.size())) {
        kosio::log::console.error("Failed to parse response");
        co_return;
    }
    if (get_response.header().success()) {
        kosio::log::console.info("{}", get_response.value());
    } else {
        kosio::log::console.error("{}", get_response.header().error());
    }
}

auto RpcDeleteCallback(std::string_view resp_payload) -> kosio::async::Task<> {
    DeleteResponse delete_response;
    if (!delete_response.ParseFromArray(resp_payload.data(), resp_payload.size())) {
        kosio::log::console.error("Failed to parse response");
        co_return;
    }
    if (delete_response.header().success()) {
        kosio::log::console.info("Success");
    } else {
        kosio::log::console.error("{}", delete_response.header().error());
    }
}

auto RpcPut(foskv::rpc::RpcConsumer* consumer, KVCommand::Args args)
-> kosio::async::Task<> {
    foskv::storage::PutRequest put_request;
    put_request.set_key(args.key);
    put_request.set_value(args.value);
    auto payload = put_request.SerializeAsString();
    auto ret = co_await consumer->call(KVRpc::ServiceName, KVRpc::Put, payload, RpcPutCallback);
    if (!ret) [[unlikely]] {
        kosio::log::console.error("Failed to call rpc {}-{} : {}", KVRpc::ServiceName, KVRpc::Put, ret.error());
    }
}

auto RpcGet(foskv::rpc::RpcConsumer* consumer, KVCommand::Args args)
-> kosio::async::Task<> {
    foskv::storage::GetRequest get_request;
    get_request.set_key(args.key);
    auto payload = get_request.SerializeAsString();
    auto ret = co_await consumer->call(KVRpc::ServiceName, KVRpc::Get, payload, RpcGetCallback);
    if (!ret) [[unlikely]] {
        kosio::log::console.error("Failed to call rpc {}-{} : {}", KVRpc::ServiceName, KVRpc::Get, ret.error());
    }
}

auto RpcDelete(foskv::rpc::RpcConsumer* consumer, KVCommand::Args args)
-> kosio::async::Task<> {
    foskv::storage::DeleteRequest delete_request;
    delete_request.set_key(args.key);
    auto payload = delete_request.SerializeAsString();
    auto ret = co_await consumer->call(KVRpc::ServiceName, KVRpc::Delete, payload, RpcDeleteCallback);
    if (!ret) [[unlikely]] {
        kosio::log::console.error("Failed to call rpc {}-{} : {}", KVRpc::ServiceName, KVRpc::Delete, ret.error());
    }
}

auto process(std::unique_ptr<foskv::rpc::RpcConsumer> consumer) -> kosio::async::Task<> {
    while (true) {
        auto args = co_await KVCommand::async_parse();
        switch (args.op) {
            case KVCommand::Op::kPut: {
                kosio::spawn(RpcPut(consumer.get(), args));
                break;
            }
            case KVCommand::Op::kGet: {
                kosio::spawn(RpcGet(consumer.get(), args));
                break;
            }
            case KVCommand::Op::kDelete: {
                kosio::spawn(RpcDelete(consumer.get(), args));
                break;
            }
            case KVCommand::Op::kExit: {
                co_await consumer->shutdown();
                co_return;
            }
            default: {
                kosio::log::console.warn("Unknown operation");
                break;
            }
        }
    }
}

auto main_loop() -> kosio::async::Task<> {
    auto has_addr = kosio::net::SocketAddr::parse("127.0.0.1", 8080);
    if (!has_addr) [[unlikely]] {
        kosio::log::console.error("Failed to parse address.");
        co_return;
    }
    auto has_consumer = co_await foskv::rpc::RpcConsumer::connect(has_addr.value());
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