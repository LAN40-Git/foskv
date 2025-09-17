#pragma once
#include "foskv/rpc/util.hpp"
#include "foskv/rpc/config.hpp"
#include <kosio/signal/signal.hpp>

namespace foskv::rpc {
class RpcProvider {
    using Invoke = std::function<RpcResult<std::size_t>(std::string_view payload, std::span<char> response)>;
    using Method = std::unordered_map<std::string_view, Invoke>;
    using Service = std::unordered_map<std::string_view, Method>;

public:
    RpcProvider(std::string_view host, uint16_t port)
        : host_(host), port_(port) {}

public:
    [[REMEMBER_CO_AWAIT]]
    auto run() -> kosio::async::Task<kosio::Result<kosio::Error>>;

public:
    void register_invoke(
        int fd,
        std::string_view service_name,
        std::string_view method_name,
        Invoke&& invoke);

private:
    auto handle_rpc(kosio::net::TcpStream stream) -> kosio::async::Task<>;
    auto invoke(
        int fd,
        std::string_view service_name,
        std::string_view method_name,
        std::string_view payload,
        std::span<char> response) -> RpcResult<std::size_t>;

private:
    std::string host_;
    uint16_t    port_;
    std::unordered_map<int, Service> invokes_;
};
} // namespace foskv::rpc