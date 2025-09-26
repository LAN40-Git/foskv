#include "foskv/rpc.hpp"
#include "foskv/raft/util.hpp"
#include "foskv/storage/storage.hpp"
using namespace foskv;
using namespace foskv::rpc;

std::chrono::steady_clock::time_point start;
std::chrono::steady_clock::time_point end;
std::atomic<uint64_t> counter{0};

auto main_loop() -> kosio::async::Task<> {
    rocksdb::Options options;
    options.create_if_missing = true;
    options.error_if_exists = false;

    auto has_st = storage::Storage::Open(options, "data");
    if (!has_st) {
        LOG_ERROR("{}", has_st.error());
        co_return;
    }

    auto st = std::move(has_st.value());

    auto has_addr = kosio::net::SocketAddr::parse("127.0.0.1", 8080);
    if (!has_addr) {
        LOG_ERROR("{}", has_addr.error());
        co_return;
    }

    RpcProvider provider(has_addr.value());
    provider.register_invoke(KVService::ServiceName, KVService::Put,
        [&st](std::string_view req_payload, std::span<char> resp_payload, uint64_t, uint64_t request_id) -> kosio::async::Task<Result<std::size_t>> {
            kv::PutRequest request;
            if (!request.ParseFromArray(req_payload.data(), req_payload.size())) {
                LOG_ERROR("Failed to parse request : {}", request_id);
                co_return raft::detail::produce_kv_put_response(resp_payload, false, RpcError::kKVPutRequestParseFailed);
            }

            auto status = st.Put(request.key(), request.value());
            if (!status.ok()) {
                LOG_ERROR("{}", status.ToString());
                co_return raft::detail::produce_kv_put_response(resp_payload, false, RpcError::kKVPutFailed);
            }
            LOG_INFO("Handle put request {}", request_id);
            if (auto ret = counter.fetch_add(1, std::memory_order_relaxed); ret % 100000 == 0) {
                end = std::chrono::steady_clock::now();
                kosio::log::console.info("Handle 10w kv put request, take {} ms, counter : {}",
                    std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count(), ret);
                start = std::chrono::steady_clock::now();
            }
            co_return raft::detail::produce_kv_put_response(resp_payload);
    });
    provider.register_invoke(KVService::ServiceName, KVService::Get,
        [&st](std::string_view req_payload, std::span<char> resp_payload, uint64_t, uint64_t) -> kosio::async::Task<Result<std::size_t>> {
            kv::GetRequest request;
            if (!request.ParseFromArray(req_payload.data(), req_payload.size())) {
                co_return raft::detail::produce_kv_put_response(resp_payload, false, RpcError::kKVGetRequestParseFailed);
            }

            std::string value;
            auto status = st.Get(request.key(), &value);
            auto kv = raft::detail::produce_kv(request.key(), std::move(value));
            if (!status.ok()) {
                LOG_ERROR("{}", status.ToString());
                co_return raft::detail::produce_kv_put_response(resp_payload, false, RpcError::kKVGetFailed);
            }
            co_return raft::detail::produce_kv_get_response(resp_payload, true, rpc::RpcError::kNoError, std::move(kv));
    });
    provider.register_invoke(KVService::ServiceName, KVService::Delete,
        [&st](std::string_view req_payload, std::span<char> resp_payload, uint64_t, uint64_t) -> kosio::async::Task<Result<std::size_t>> {
            kv::DeleteRequest request;
            if (!request.ParseFromArray(req_payload.data(), req_payload.size())) {
                co_return raft::detail::produce_kv_put_response(resp_payload, false, RpcError::kKVDeleteRequestParseFailed);
            }

            auto status = st.Delete(request.key());
            if (!status.ok()) {
                LOG_ERROR("{}", status.ToString());
                co_return raft::detail::produce_kv_put_response(resp_payload, false, RpcError::kKVDeleteFailed);
            }
            co_return raft::detail::produce_kv_delete_response(resp_payload);
    });
    co_await provider.run();
}

auto main() -> int {
    SET_LOG_LEVEL(kosio::log::LogLevel::Verbose);
    kosio::runtime::MultiThreadBuilder::options().set_num_workers(16).build().block_on(main_loop());
}