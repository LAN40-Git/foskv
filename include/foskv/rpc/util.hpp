#pragma once
#include "foskv/proto/rpc.pb.h"
#include "foskv/rpc/config.hpp"
#include "foskv/common/error.hpp"
#include "foskv/common/util/concurrent_queue.hpp"
#include "foskv/common/util/noncopyable.hpp"
#include <functional>
#include <xxhash.h>
#include <kosio/net.hpp>

namespace foskv::rpc::detail {
// addr -> request_id -> req_payload -> resp_payload
using Invoke = std::function<kosio::async::Task<RpcResult<std::size_t>>(std::string_view, uint64_t, std::string_view, std::span<char>)>;
using Service = std::unordered_map<std::string_view, Invoke>;
class InvokeTask : util::Noncopyable {
public:
    InvokeTask() = default;
    explicit InvokeTask(uint64_t request_id, Invoke&& invoke, std::string&& req_payload, bool has_resp_payload, std::string&& resp_payload)
    : request_id_(request_id)
    , invoke_(std::move(invoke))
    , req_payload_(std::move(req_payload))
    , has_resp_payload_(has_resp_payload)
    , resp_payload_(std::move(resp_payload)) {}

    InvokeTask(InvokeTask&& other) noexcept
        : request_id_(other.request_id_)
        , invoke_(std::move(other.invoke_))
        , req_payload_(std::move(other.req_payload_))
        , has_resp_payload_(other.has_resp_payload_)
        , resp_payload_(std::move(other.resp_payload_)) {}
    auto operator=(InvokeTask&& other) noexcept -> InvokeTask& {
        request_id_ = other.request_id_;
        invoke_ = std::move(other.invoke_);
        req_payload_ = std::move(other.req_payload_);
        has_resp_payload_ = other.has_resp_payload_;
        resp_payload_ = std::move(other.resp_payload_);
        return *this;
    }

public:
    uint64_t request_id_{};
    Invoke invoke_;
    std::string req_payload_;
    bool has_resp_payload_{false};
    std::string resp_payload_;
};

using RpcCallback = std::function<kosio::async::Task<>(std::string_view resp_payload)>;
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

struct SocketAddrXXHash {
    std::size_t operator()(const kosio::net::SocketAddr& addr) const noexcept {
        thread_local XXH64_state_t* state = XXH64_createState();
        XXH64_reset(state, 0);

        int family = addr.family();
        XXH64_update(state, &family, sizeof(family));

        uint16_t port = addr.port();
        XXH64_update(state, &port, sizeof(port));

        auto ip = addr.ip();
        if (const kosio::net::Ipv4Addr* ipv4 = std::get_if<kosio::net::Ipv4Addr>(&ip)) {
            uint32_t ip_val = ipv4->addr();
            XXH64_update(state, &ip_val, sizeof(ip_val));
        } else if (const kosio::net::Ipv6Addr* ipv6 = std::get_if<kosio::net::Ipv6Addr>(&ip)) {
            const in6_addr& in6 = ipv6->addr();
            XXH64_update(state, &in6, sizeof(in6));
        }

        auto hash = XXH64_digest(state);
        XXH64_freeState(state);
        return hash;
    }
};
} // namespace foskv::rpc::detail