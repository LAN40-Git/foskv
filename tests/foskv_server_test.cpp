#include "foskv/raft/raft_node.hpp"
#include "kosio/signal.hpp"

auto server() -> kosio::async::Task<> {
    auto has_consumer = co_await foskv::rpc::RpcConsumer::connect("127.0.0.1", 8080);
    if (!has_consumer) {
        kosio::log::console.error("Failed to connect to server : {}", has_consumer.error());
    }
    kosio::log::console.info("Connected");
    co_await std::suspend_always {};
}


auto main_loop() -> kosio::async::Task<> {
    kosio::spawn(server());
    co_await kosio::signal::ctrl_c();
}

auto main() -> int {
    kosio::runtime::CurrentThreadBuilder::default_create().block_on(server());
}