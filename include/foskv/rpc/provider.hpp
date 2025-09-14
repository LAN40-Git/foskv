#pragma once
#include "foskv/common/error.hpp"
#include "foskv/rpc/rpc.pb.h"
#include <functional>
#include <unordered_map>
#include <kosio/core.hpp>
#include <kosio/net.hpp>

namespace foskv::rpc {
class RpcProvider {
    using Invoke = std::function<void(std::string_view payload, std::string& response)>;

public:
    RpcProvider(std::string_view host, uint16_t port)
        : host_(host), port_(port) {}

public:
    void register_invoke(const std::string& service_name,
        const std::string& method_name, const Invoke &invoke);
    void run();

private:
    auto handle_rpc(kosio::net::TcpStream stream) -> kosio::async::Task<>;
    auto invoke(const std::string& service_name, const std::string& method_name,
        std::string_view payload) -> RpcResult<std::string>;

private:
    std::string host_;
    uint16_t    port_;
    std::string request_str_;
    // <service_name, <method_name, Method>>
    std::unordered_map<std::string, std::unordered_map<std::string, Invoke>> invokes_;
};
} // namespace foskv::rpc