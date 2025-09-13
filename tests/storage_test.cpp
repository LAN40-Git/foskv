#include "foskv/storage/storage.hpp"
#include "foskv/storage/command.hpp"
#include <kosio/core.hpp>
#include <kosio/net.hpp>
#include <kosio/log.hpp>
#include <kosio/signal.hpp>

auto process(kosio::net::TcpStream stream, foskv::storage::KVStorage& st) -> kosio::async::Task<> {
    char buf[1024];
    while (true) {
        auto ret = co_await stream.read(buf);
        if (!ret || ret.value() == 0) {
            if (ret.value() == 0) {
                kosio::log::console.info("Client closed : {}", stream.peer_addr().value());
            } else {
                kosio::log::console.error("{}", ret.error());
            }
            break;
        }
        auto len = ret.value();
        auto start = std::chrono::system_clock::now();
        auto args = foskv::storage::KVCommand::parse({buf, len});

        switch (args.op) {
            case foskv::storage::KVCommand::Op::kPut: {
                auto status = st.Put(args.key, args.value);
                co_await stream.write_all(status.ToString()+'\n');
                break;
            }
            case foskv::storage::KVCommand::Op::kGet: {
                auto status = st.Get(args.key, &args.value);
                if (!status.ok()) {
                    co_await stream.write_all(status.ToString()+'\n');
                } else {
                    co_await stream.write_all(args.value + '\n');
                }
                break;
            }
            case foskv::storage::KVCommand::Op::kDelete: {
                auto status = st.Delete(args.key);
                co_await stream.write_all(status.ToString()+'\n');
                break;
            }
            default: {
                kosio::log::console.error("Unknown kv command");
                break;
            }
        }
        auto end = std::chrono::system_clock::now();
        kosio::log::console.info("Take {} ns", std::chrono::duration_cast<std::chrono::microseconds>(end - start).count());
    }
}

auto storage_server(foskv::storage::KVStorage& st) -> kosio::async::Task<> {
    auto has_addr = kosio::net::SocketAddr::parse("localhost", 8080);
    if (!has_addr) {
        kosio::log::console.error("{}", has_addr.error());
        co_return;
    }
    auto has_listener = kosio::net::TcpListener::bind(has_addr.value());
    if (!has_listener) {
        kosio::log::console.error("{}", has_listener.error());
        co_return;
    }
    auto listener = std::move(has_listener.value());
    while (true) {
        auto ret = co_await listener.accept();
        if (!ret) {
            kosio::log::console.error("{}", ret.error());
            break;
        }
        auto& [stream, _] = ret.value();
        kosio::spawn(process(std::move(stream), st));
    }
}

auto main_loop(foskv::storage::KVStorage& st) -> kosio::async::Task<> {
    kosio::spawn(storage_server(st));
    co_await kosio::signal::ctrl_c();
    kosio::log::console.info("Closing...");
    co_await kosio::time::sleep(100);
    kosio::log::console.info("Closed");
}

auto main() -> int {
    auto has_storage = foskv::storage::KVStorage::Open("./test_db");
    if (!has_storage) {
        return -1;
    }
    auto st = std::move(has_storage.value());
    kosio::runtime::MultiThreadBuilder::default_create().block_on(main_loop(st));
    return 0;
}