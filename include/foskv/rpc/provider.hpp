#pragma once
#include "foskv/common/error.hpp"
#include "foskv/rpc/rpc.pb.h"
#include <functional>
#include <unordered_map>
#include <kosio/core.hpp>
#include <kosio/net.hpp>

namespace foskv::rpc {
class RpcProvider {
    using Handler = std::function<void(std::string_view args, std::string& response)>;

public:
    RpcProvider(std::string_view host, uint16_t port)
        : host_(host), port_(port) {}

public:
    void register_handler(const std::string& service_name, const std::string& method_name, const Handler &handler);
    auto invoke(const std::string& service_name, const std::string& method_name, std::string_view args, std::string& response)
    -> RpcResult<void>;
    void run();

private:
    auto handle_rpc(kosio::net::TcpStream stream) -> kosio::async::Task<>;

private:
    std::string host_;
    uint16_t    port_;
    // <service_name, <method_name, Handler>>
    std::unordered_map<std::string, std::unordered_map<std::string, Handler>> invokers_;
};
} // namespace foskv::rpc