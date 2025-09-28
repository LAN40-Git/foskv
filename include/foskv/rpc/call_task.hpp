#pragma once
#include "foskv/rpc/util.hpp"

namespace foskv::rpc {
using RpcCallback = std::function<kosio::async::Task<>(std::string_view resp_payload)>;
namespace detail {
using RpcCallbackMap = tbb::concurrent_hash_map<uint64_t, RpcCallback>;
class CallTask : util::Noncopyable {
public:
    CallTask() = default;
    explicit CallTask(ServiceType service_type, MethodType method_type, std::string_view req_payload, const RpcCallback& callback)
        : service_type_(service_type)
        , method_type_(method_type)
        , req_payload_(std::string{req_payload})
        , callback_(callback) {}

    explicit CallTask(ServiceType service_type, MethodType method_type, std::string_view req_payload, RpcCallback&& callback)
        : service_type_(service_type)
        , method_type_(method_type)
        , req_payload_(std::string{req_payload})
        , callback_(std::move(callback)) {}

    explicit CallTask(ServiceType service_type, MethodType method_type, std::string&& req_payload, RpcCallback&& callback)
        : service_type_(service_type)
        , method_type_(method_type)
        , req_payload_(std::move(req_payload))
        , callback_(std::move(callback)) {}

    // Delete copy
    CallTask(const CallTask&) = delete;
    auto operator=(const CallTask&) = delete;

    CallTask(CallTask&& other) noexcept
        : service_type_(other.service_type_)
        , method_type_(other.method_type_)
        , req_payload_(std::move(other.req_payload_))
        , callback_(std::move(other.callback_)) {}

    auto operator=(CallTask&& other) noexcept -> CallTask& {
        service_type_ = other.service_type_;
        method_type_ = other.method_type_;
        req_payload_ = std::move(other.req_payload_);
        callback_ = std::move(other.callback_);
        return *this;
    }

public:
    ServiceType service_type_{};
    MethodType  method_type_{};
    std::string req_payload_{};
    RpcCallback callback_;
};
} // namespace detail
} // namespace foskv::rpc