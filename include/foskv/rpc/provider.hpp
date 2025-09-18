#pragma once
#include "foskv/rpc/util.hpp"
#include "foskv/rpc/config.hpp"
#include <kosio/signal/signal.hpp>

namespace foskv::rpc {
class RpcProvider {
    // Use ParseFromArray(req_payload.data(), req_payload.size()) to get the rpc request.
    // Use SerializeToArray(resp_payload.data(), resp_payload_size) to write the rpc
    // response, return error if resp_payload_size > resp_payload.size() or failed
    // to serialize
    using Invoke = std::function<kosio::async::Task<RpcResult<std::size_t>>(std::string_view req_payload, std::span<char> resp_payload)>;
    using Service = std::unordered_map<std::string_view, Invoke>;

public:
    explicit RpcProvider(const kosio::net::SocketAddr& addr)
        : addr_(addr) {}

public:
    /// @brief Asynchronous accept rpc client connection
    /// @return A coro task, asynchronous accept rpc client connection
    /// @note Remember `co_await`, you may wrap this task with another coro
    ///       which has reconnect logic. Not thread-safe
    [[REMEMBER_CO_AWAIT]]
    auto run() -> kosio::async::Task<kosio::Result<kosio::Error>>;

public:
    /// @brief Register a rpc invoke
    /// @param service_name Service name
    /// @param method_name Method name
    /// @param invoke The invoke function
    /// @note Not thread-safe
    void register_invoke(
        std::string_view service_name,
        std::string_view method_name,
        Invoke&& invoke);

private:
    /// @brief Handle the rpc request from client
    /// @param stream TcpStream from rpc client
    /// @return A coro task which handle the rpc request from rpc client,
    ///         remember to spawn this task
    auto handle_rpc(kosio::net::TcpStream stream) -> kosio::async::Task<>;

private:
    kosio::net::SocketAddr addr_;
    // service_name -> method_name -> invoke
    std::unordered_map<std::string_view, Service> invokes_;
};
} // namespace foskv::rpc