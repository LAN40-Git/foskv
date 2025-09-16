#include "foskv/rpc/provider.hpp"
#include "foskv/storage/storage.hpp"

auto main() -> int {
    auto has_st = foskv::storage::KVStorage::Open("./test_db");
    if (!has_st) {
        kosio::log::console.error("{}", has_st.error());
        return -1;
    }
    auto st = std::move(has_st.value());

    foskv::rpc::RpcProvider provider{"127.0.0.1", 8080};
    provider.register_invoke("KVStorage", "Put", [&st](std::string_view payload, std::string& response) {
        st.RpcPut(payload, response);
    });
    provider.register_invoke("KVStorage", "Get", [&st](std::string_view payload, std::string& response) {
        st.RpcGet(payload, response);
    });
    provider.register_invoke("KVStorage", "Delete", [&st](std::string_view payload, std::string& response) {
        st.RpcDelete(payload, response);
    });

    kosio::runtime::MultiThreadBuilder::default_create().block_on(provider.event_loop());
    return 0;
}