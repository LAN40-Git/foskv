#pragma once
#include "foskv/common/error.hpp"
#include "foskv/rpc/rpc.pb.h"
#include <kosio/core.hpp>
#include <kosio/net.hpp>
#include <functional>
#include <kosio/sync.hpp>

namespace foskv::rpc {
using RpcCallback = std::function<void(const std::string&)>;
class RpcConsumer {
public:
    explicit RpcConsumer(kosio::net::TcpStream&& stream)
        : stream_(std::move(stream)) {
        kosio::spawn(run());
    }

public:
    // Try to use temporary constructs to avoid copying
    struct RpcTask {
        std::string service_name;
        std::string method_name;
        std::string payload;
        RpcCallback callback;
    };

public:
    static auto connect(std::string_view host, uint16_t port)
    -> kosio::async::Task<RpcResult<std::unique_ptr<RpcConsumer>>>;

public:
    // Pull a rpc task to the tasks queue, thread safe.
    [[REMEMBER_CO_AWAIT]]
    auto call(std::string&& service_name,
              std::string&& method_name,
              std::string&& payload,
              const std::function<void(const std::string&)>& callback) -> kosio::async::Task<>;

    auto run() -> kosio::async::Task<void> {
        while (true) {
            co_await mutex_.lock();
            std::unique_lock lock(mutex_, std::adopt_lock);
            co_await cv_.wait(mutex_, [this]() {
                return !rpc_tasks_.empty();
            });
            while (!rpc_tasks_.empty()) {
                auto rpc_task = rpc_tasks_.front();
                rpc_tasks_.pop();
                auto has_response = co_await this->call_internal(rpc_task.service_name, rpc_task.method_name, rpc_task.payload);
                if (!has_response) [[unlikely]] {
                    LOG_ERROR("{}", has_response.error());
                    co_return;
                }
                rpc_task.callback(has_response.value());
            }
        }
    }

private:
    [[REMEMBER_CO_AWAIT]]
    auto call_internal(const std::string& service_name,
                       const std::string& method_name,
                       const std::string& payload) -> kosio::async::Task<RpcResult<std::string>>;

private:
    kosio::sync::ConditionVariable cv_;
    kosio::sync::Mutex             mutex_;
    kosio::net::TcpStream          stream_;
    std::queue<RpcTask>            rpc_tasks_;
};
} // namespace foskv::rpc