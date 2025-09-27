#pragma once
#include "foskv/rpc.hpp"
#include "foskv/raft/config.hpp"

namespace foskv::raft::detail {
class ApplyTask {
public:
    explicit ApplyTask(uint64_t last_applied, uint64_t session_id, uint64_t request_id, InternalRaftRequest&& request)
        : last_applied_(last_applied)
        , session_id_(session_id)
        , request_id_(request_id)
        , request_(std::move(request)) {}

    // Delete copy
    ApplyTask(const ApplyTask&) = delete;
    auto operator=(const ApplyTask&) -> ApplyTask& = delete;

    // Allow move
    ApplyTask(ApplyTask&&) = default;
    auto operator=(ApplyTask&&) -> ApplyTask& = default;

public:
    uint64_t            last_applied_;
    uint64_t            session_id_;
    uint64_t            request_id_;
    InternalRaftRequest request_;
};

[[nodiscard]]
static auto produce_log_entry(uint64_t current_term,
    uint64_t log_index, std::string&& command) -> LogEntry {
    LogEntry entry;
    entry.set_term(current_term);
    entry.set_index(log_index);
    entry.set_command(std::move(command));
    return entry;
}

[[nodiscard]]
static auto produce_redirect(std::string&& host, uint16_t port) -> rpc::Redirect {
    rpc::Redirect redirect;
    redirect.mutable_host()->swap(host);
    redirect.set_port(port);
    return redirect;
}

[[nodiscard]]
static auto produce_kv(std::string&& key, std::string&& value) -> kv::KeyValue {
    kv::KeyValue kv;
    kv.set_key(std::move(key));
    kv.set_value(std::move(value));
    return kv;
}

[[nodiscard]]
static auto produce_kv(const std::string& key, std::string&& value) -> kv::KeyValue {
    kv::KeyValue kv;
    kv.set_key(key);
    kv.set_value(std::move(value));
    return kv;
}

[[nodiscard]]
static auto produce_rpc_response_header(bool success, uint32_t error_code,
    std::optional<rpc::Redirect>&& redirect) -> rpc::ResponseHeader {
    rpc::ResponseHeader header;
    header.set_success(success);
    header.set_error_code(error_code);
    if (redirect) {
        header.mutable_redirect()->Swap(&redirect.value());
    }
    return header;
}

[[nodiscard]]
static auto produce_kv_put_response(std::span<char> resp_payload, bool success = true, uint32_t error_code = rpc::RpcError::kNoError,
    std::optional<rpc::Redirect>&& redirect = std::nullopt) -> Result<std::size_t> {
    kv::PutResponse response;
    auto header = produce_rpc_response_header(success, error_code, std::move(redirect));
    response.mutable_header()->Swap(&header);
    auto resp_payload_size = response.ByteSizeLong();
    if (!response.SerializeToArray(resp_payload.data(), static_cast<int>(resp_payload_size))) {
        return std::unexpected{make_error(Error::kKVPutResponseSerializeFailed)};
    }
    return resp_payload_size;
}

[[nodiscard]]
static auto produce_kv_get_response(std::span<char> resp_payload, bool success = true, uint32_t error_code = rpc::RpcError::kNoError,
        std::optional<kv::KeyValue>&& kv = std::nullopt, std::optional<rpc::Redirect>&& redirect = std::nullopt) -> Result<std::size_t> {
    kv::GetResponse response;
    auto header = produce_rpc_response_header(success, error_code, std::move(redirect));
    response.mutable_header()->Swap(&header);
    if (kv) {
        response.mutable_kv()->Swap(&kv.value());
    }
    auto resp_payload_size = response.ByteSizeLong();
    if (!response.SerializeToArray(resp_payload.data(), static_cast<int>(resp_payload_size))) {
        return std::unexpected{make_error(Error::kKVGetResponseSerializeFailed)};
    }
    return resp_payload_size;
}

[[nodiscard]]
static auto produce_kv_delete_response(std::span<char> resp_payload, bool success = true, uint32_t error_code = rpc::RpcError::kNoError,
        std::optional<rpc::Redirect>&& redirect = std::nullopt) -> Result<std::size_t> {
    kv::DeleteResponse response;
    auto header = produce_rpc_response_header(success, error_code, std::move(redirect));
    response.mutable_header()->Swap(&header);
    auto resp_payload_size = response.ByteSizeLong();
    if (!response.SerializeToArray(resp_payload.data(), static_cast<int>(resp_payload_size))) {
        return std::unexpected{make_error(Error::kKVDeleteResponseSerializeFailed)};
    }
    return resp_payload_size;
}
} // namespace foskv::raft::detail