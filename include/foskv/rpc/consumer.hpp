#pragma once
#include "foskv/rpc/util.hpp"
#include <kosio/sync.hpp>
#include <tbb/concurrent_unordered_map.h>

namespace foskv::rpc {
// One RpcConsumer will approximately cost 8MB
// Remember to co_await close(), otherwise,
// there is a risk of the program crashing
class RpcConsumer {
public:
    explicit RpcConsumer(kosio::net::TcpStream&& stream)
        : buffer_(4 * 1024 * 1024) // 4MB
        , stream_(std::move(stream)) {
        server_addr_ = stream_.peer_addr().value();
        LOG_INFO("Connected to {}", server_addr_);
        kosio::spawn(run());
    }

    // Remeber to co_await close.
    ~RpcConsumer() = default;

public:
    static auto connect(std::string_view host, uint16_t port)
    -> kosio::async::Task<RpcResult<std::unique_ptr<RpcConsumer>>>;

public:
    /// Call a rpc method, thread safe.
    [[REMEMBER_CO_AWAIT]]
    auto call(std::string&& service_name,
              std::string&& method_name,
              std::string&& payload,
              detail::RpcCallback&& callback) -> kosio::async::Task<RpcResult<void>>;
    [[REMEMBER_CO_AWAIT]]
    auto reconnect() -> kosio::async::Task<bool>;
    /// This function must be called at the end!!!
    [[REMEMBER_CO_AWAIT]]
    auto close() -> kosio::async::Task<>;

private:
    auto run() -> kosio::async::Task<>;

private:
    kosio::sync::Mutex                                           mutex_;
    kosio::sync::Latch                                           latch_{1};
    uint64_t                                                     request_id_{0};
    std::vector<char>                                            buffer_;
    kosio::net::TcpStream                                        stream_;
    kosio::net::SocketAddr                                       server_addr_;
    tbb::concurrent_unordered_map<uint64_t, detail::RpcCallback> callbacks_;
};
} // namespace foskv::rpc