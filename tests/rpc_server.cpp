#include "foskv/rpc/provider.hpp"
#include "foskv/storage/storage.hpp"
#include <nlohmann/json.hpp>

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
    // 创建 JSON 对象并设置值
    nlohmann::json config;
    config["ip"] = "127.0.0.1";
    config["port"] = "8080";

    // 打开文件流
    std::ofstream out_file("config.json");
    if (!out_file.is_open()) {
        std::cerr << "无法打开文件进行写入" << std::endl;
        return 1;
    }

    // 将 JSON 对象写入文件
    out_file << config.dump(4);  // 参数 4 表示缩进为 4 个空格，使输出更易读

    // 关闭文件
    out_file.close();
    kosio::runtime::MultiThreadBuilder::default_create().block_on(main_loop());
    return 0;
}