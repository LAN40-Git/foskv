#pragma once
#include "foskv/rpc/util.hpp"

namespace foskv::rpc::detail {
class InvokeTask;
// req_payload -> resp_payload -> session_id -> request_id
using Invoke = std::function<kosio::async::Task<Result<std::size_t>>(std::string_view, std::span<char>, uint64_t, uint64_t)>;
using InvokeMap = std::unordered_map<std::string_view, std::unordered_map<std::string_view, Invoke>>;
class InvokeTask : util::Noncopyable {
public:
    InvokeTask() = default;
    explicit InvokeTask(Invoke&& invoke)
        : invoke_(std::move(invoke)) {}

    explicit InvokeTask(uint64_t request_id, Invoke&& invoke)
    : request_id_(request_id)
    , invoke_(std::move(invoke)) {}

    explicit InvokeTask(uint64_t request_id, Invoke&& invoke, std::string&& req_payload)
    : request_id_(request_id)
    , invoke_(std::move(invoke))
    , req_payload_(std::move(req_payload)) {}

    InvokeTask(InvokeTask&& other) noexcept
        : request_id_(other.request_id_)
        , invoke_(std::move(other.invoke_))
        , req_payload_(std::move(other.req_payload_)) {}
    auto operator=(InvokeTask&& other) noexcept -> InvokeTask& {
        request_id_ = other.request_id_;
        invoke_ = std::move(other.invoke_);
        req_payload_ = std::move(other.req_payload_);
        return *this;
    }

public:
    uint64_t request_id_{};
    Invoke invoke_;
    std::string req_payload_{};
};
} // namespace foskv::rpc::detail