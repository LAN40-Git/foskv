#include "foskv/rpc/provider.hpp"
#include "foskv/storage/storage.hpp"

auto server() -> kosio::async::Task<> {
    auto has_st = foskv::storage::KVStorage::Open("./test_db");
    if (!has_st) {
        kosio::log::console.error("{}", has_st.error());
        co_return;
    }
    auto st = std::move(has_st.value());

    foskv::rpc::RpcProvider provider{"127.0.0.1", 8080};
    provider.register_invoke("KVStorage", "Put", [&st](std::string_view payload, std::span<char> response) -> foskv::RpcResult<std::size_t> {
        return st.RpcPut(payload, response);
    });
    provider.register_invoke("KVStorage", "Get", [&st](std::string_view payload, std::span<char> response) -> foskv::RpcResult<std::size_t> {
        return st.RpcGet(payload, response);
    });
    provider.register_invoke("KVStorage", "Delete", [&st](std::string_view payload, std::span<char> response) -> foskv::RpcResult<std::size_t> {
        return st.RpcDelete(payload, response);
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