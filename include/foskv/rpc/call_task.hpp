#pragma once
#include "foskv/rpc/util.hpp"

namespace foskv::rpc {
using RpcCallback = std::function<kosio::async::Task<>(std::string_view resp_payload)>;
namespace detail {
using RpcCallbackMap = std::unordered_map<uint64_t, RpcCallback>;
class CallTask : util::Noncopyable {
public:
    CallTask() = default;
    explicit CallTask(std::string&& service_name, std::string&& method_name, std::string&& req_payload, RpcCallback&& callback)
        : service_name_(std::move(service_name))
        , method_name_(std::move(method_name))
        , req_payload_(std::move(req_payload))
        , callback_(std::move(callback)) {}

    CallTask(CallTask&& other) noexcept
        : service_name_(std::move(other.service_name_))
        , method_name_(std::move(other.method_name_))
        , req_payload_(std::move(other.req_payload_))
        , callback_(std::move(other.callback_)) {}

    auto operator=(CallTask&& other) noexcept -> CallTask& {
        service_name_ = std::move(other.service_name_);
        method_name_ = std::move(other.method_name_);
        req_payload_ = std::move(other.req_payload_);
        callback_ = std::move(other.callback_);
        return *this;
    }

public:
    std::string service_name_;
    std::string method_name_;
    std::string req_payload_;
    RpcCallback callback_;
};
} // namespace detail
} // namespace foskv::rpc