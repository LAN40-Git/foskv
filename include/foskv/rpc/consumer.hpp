#pragma once
#include "foskv/common/error.hpp"
#include "foskv/rpc/rpc.pb.h"
#include <functional>
#include <kosio/core.hpp>
#include <kosio/net.hpp>

namespace foskv::rpc {
class RpcConsumer {
public:
    explicit RpcConsumer(kosio::net::TcpStream&& stream)
        : stream_(std::move(stream)) {}

public:
    static auto connect(std::string_view host, uint16_t port)
    -> kosio::async::Task<RpcResult<RpcConsumer>>;

public:
    template <typename Response>
         requires std::is_base_of_v<google::protobuf::Message, Response>
    auto call(const std::string service_name,
              const std::string method_name,
              const std::string payload,
              std::function<kosio::async::Task<>(RpcResult<Response>)> callback) -> kosio::async::Task<> {
        co_await callback(co_await internal_call<Response>(service_name, method_name, payload));
    }

    template <typename Response>
         requires std::is_base_of_v<google::protobuf::Message, Response>
    auto call(const std::string service_name,
              const std::string method_name,
              const std::string payload,
              std::function<void(RpcResult<Response>)> callback) -> kosio::async::Task<> {
        callback(co_await internal_call<Response>(service_name, method_name, payload));
    }

private:
    template <typename Response>
         requires std::is_base_of_v<google::protobuf::Message, Response>
    [[REMEMBER_CO_AWAIT]]
    auto internal_call(const std::string& service_name,
                       const std::string& method_name,
                       const std::string& payload) -> kosio::async::Task<RpcResult<Response>> {
        // Make request
        RpcRequest request;
        request.set_service_name(service_name);
        request.set_method_name(std::string(method_name));
        request.set_payload(payload);

        // Send request
        std::string request_data = request.SerializeAsString();
        uint32_t request_len_net = htonl(static_cast<uint32_t>(request_data.size()));

        auto ret = co_await stream_.write_vectored(
            std::span<const char>(reinterpret_cast<char*>(&request_len_net), sizeof(uint32_t)),
            std::span<const char>(request_data.data(), request_data.size())
        );

        if (!ret) [[unlikely]] {
            co_return std::unexpected{make_rpc_error(RpcError::kSendFailed)};
        }

        // Recv response length
        uint32_t response_len_net;
        auto has_response_len = co_await stream_.read_exact(
            {reinterpret_cast<char*>(&response_len_net), sizeof(uint32_t)});
        if (!has_response_len) [[unlikely]] {
            co_return std::unexpected{make_rpc_error(RpcError::kReceiveFailed)};
        }

        uint32_t response_len = ntohl(response_len_net);

        // Recv response
        std::string response_str;
        response_str.resize(response_len);
        auto has_response = co_await stream_.read_exact(
            {response_str.data(), response_len});
        if (!has_response) [[unlikely]] {
            co_return std::unexpected{make_rpc_error(RpcError::kReceiveFailed)};
        }

        Response response;
        if (!response.ParseFromArray(response_str.data(), response_len)) {
            co_return std::unexpected{make_rpc_error(RpcError::kSerializeFailed)};
        }
        co_return response;
    }

private:
    kosio::net::TcpStream stream_;
};
} // namespace foskv::rpc