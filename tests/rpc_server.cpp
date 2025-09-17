#include "foskv/rpc/provider.hpp"
#include "foskv/storage/storage.hpp"

using namespace foskv::storage;

auto server() -> kosio::async::Task<> {
    auto has_st = KVStorage::Open("./test_db");
    if (!has_st) {
        kosio::log::console.error("{}", has_st.error());
        co_return;
    }
    auto st = std::move(has_st.value());

    auto has_addr = kosio::net::SocketAddr::parse("127.0.0.1", 8080);
    if (!has_addr) {
        kosio::log::console.error("{}", has_addr.error());
        co_return;
    }

    foskv::rpc::RpcProvider provider{has_addr.value()};
    provider.register_invoke(KVRpc::ServiceName, KVRpc::Put,
        [&st](std::string_view payload, std::span<char> response) -> kosio::async::Task<foskv::RpcResult<std::size_t>>  {
        co_return st.RpcPut(payload, response);
    });
    provider.register_invoke(KVRpc::ServiceName, KVRpc::Get,
        [&st](std::string_view payload, std::span<char> response) -> kosio::async::Task<foskv::RpcResult<std::size_t>> {
        co_return st.RpcGet(payload, response);
    });
    provider.register_invoke(KVRpc::ServiceName, KVRpc::Delete,
        [&st](std::string_view payload, std::span<char> response) -> kosio::async::Task<foskv::RpcResult<std::size_t>> {
        co_return st.RpcDelete(payload, response);
    });
    auto ret = co_await provider.run();
    if (!ret) [[unlikely]] {
        kosio::log::console.error("{}", ret.error());
    }
}

auto main_loop() -> kosio::async::Task<> {
    kosio::spawn(server());
    co_await kosio::signal::ctrl_c();
}

auto main() -> int {
    kosio::runtime::MultiThreadBuilder::default_create().block_on(main_loop());
    return 0;
}