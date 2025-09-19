#pragma once
#include "foskv/proto/rpc.pb.h"
#include "foskv/rpc/config.hpp"
#include "foskv/common/error.hpp"
#include "foskv/common/util/concurrent_queue.hpp"
#include "foskv/common/util/noncopyable.hpp"
#include <kosio/net.hpp>
#include <kosio/core.hpp>
#include <functional>

namespace foskv::rpc::detail {
using Invoke = std::function<kosio::async::Task<RpcResult<std::size_t>>(std::string_view req_payload, std::span<char> resp_payload)>;
using Service = std::unordered_map<std::string_view, Invoke>;
class InvokeTask : util::Noncopyable {
public:
    InvokeTask() = default;
    explicit InvokeTask(uint64_t request_id, Invoke&& invoke, std::string&& req_payload)
        : request_id_(request_id), invoke_(std::move(invoke)), req_payload_(std::move(req_payload)) {
    }

public:
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
    std::string req_payload_;
};
} // namespace foskv::rpc::detail