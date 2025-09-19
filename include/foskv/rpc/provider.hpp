#pragma once
#include "foskv/proto/rpc.pb.h"
#include "foskv/rpc/config.hpp"
#include "foskv/common/error.hpp"
#include "foskv/common/util/noncopyable.hpp"
#include <kosio/net.hpp>
#include <functional>

namespace foskv::rpc {
class RpcProvider : util::Noncopyable {
    // Use ParseFromArray(req_payload.data(), req_payload.size()) to get the rpc request.
    // Use SerializeToArray(resp_payload.data(), resp_payload_size) to write the rpc
    // response and let resp_payload = resp_payload.subspan{0, resp_payload_size}, return
    // error if resp_payload_size > resp_payload.size() or failed to serialize
    using Invoke = std::function<kosio::async::Task<RpcResult<void>>(std::string_view req_payload, std::span<char> resp_payload)>;
    using Service = std::unordered_map<std::string_view, Invoke>;

public:
    explicit RpcProvider(const kosio::net::SocketAddr& addr)
        : addr_(addr) {}

public:
    RpcProvider(RpcProvider&& other) noexcept;
    auto operator=(RpcProvider&& other) noexcept -> RpcProvider&;

public:
    /// @brief Asynchronous accept rpc client connection
    /// @return A coro task, asynchronous accept rpc client connection
    /// @note Remember `co_await`. This function throws an exception when
    ///       it goes wrong, and you need to catch and directly execute
    ///       the logic of the program exit
    [[REMEMBER_CO_AWAIT]]
    auto run() -> kosio::async::Task<>;

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