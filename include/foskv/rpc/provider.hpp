#pragma once
#include "foskv/rpc/util.hpp"
#include "foskv/rpc/config.hpp"
#include <kosio/signal/signal.hpp>

namespace foskv::rpc {
class RpcProvider {
    using Invoke = std::function<RpcResult<std::size_t>(std::string_view payload, std::span<char> response)>;
    using Service = std::unordered_map<std::string_view, Invoke>;

public:
    RpcProvider(const kosio::net::SocketAddr& addr)
        : addr_(addr) {}

public:
    [[REMEMBER_CO_AWAIT]]
    auto run() -> kosio::async::Task<kosio::Result<kosio::Error>>;

public:
    void register_invoke(
        std::string_view service_name,
        std::string_view method_name,
        Invoke&& invoke);

private:
    auto handle_rpc(kosio::net::TcpStream stream) -> kosio::async::Task<>;
    auto invoke(
        std::string_view service_name,
        std::string_view method_name,
        std::string_view payload,
        std::span<char> response) -> RpcResult<std::size_t>;

private:
    kosio::net::SocketAddr addr_;
    // service_name -> method_name -> invoke
    std::unordered_map<std::string_view, Service> invokes_;
};
} // namespace foskv::rpc