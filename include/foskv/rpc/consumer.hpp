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
    -> kosio::async::Task<kosio::Result<RpcConsumer, kosio::IoError>>;

public:
    template <typename Response>
         requires std::is_base_of_v<google::protobuf::Message, Response>
    [[REMEMBER_CO_AWAIT]]
    auto call(const std::string& service_name,
              const std::string& method_name,
              const std::string& payload)
        -> kosio::async::Task<RpcResult<Response>> {
        // Make request
        RpcRequest request;
        request.set_service_name(service_name);
        request.set_method_name(std::string(method_name));
        request.set_payload(payload);

        // Send request length
        uint32_t header_len_net = htonl(request.ByteSizeLong());
        auto ret = co_await stream_.write_all(
            {reinterpret_cast<char*>(&header_len_net), sizeof(uint32_t)});
        if (!ret) [[unlikely]] {
            co_return std::unexpected{make_rpc_error(RpcError::kSendFailed)};
        }

        // Send request
        ret = co_await stream_.write_all(request.SerializeAsString());
        if (!ret) [[unlikely]] {
            co_return std::unexpected{make_rpc_error(RpcError::kSendFailed)};
        }

        // Read response length
        uint32_t response_len_net;
        auto has_response_len = co_await stream_.read_exact(
            {reinterpret_cast<char*>(&response_len_net), sizeof(uint32_t)});
        if (!has_response_len) [[unlikely]] {
            co_return std::unexpected{make_rpc_error(RpcError::kSendFailed)};
        }

        uint32_t response_len = ntohl(response_len_net);

        // Read response
        if (response_str_.size() < response_len) {
            response_str_.resize(response_len);
        }
        auto has_response = co_await stream_.read_exact(
            {response_str_.data(), response_len});
        if (!has_response) [[unlikely]] {
            co_return std::unexpected{make_rpc_error(RpcError::kSendFailed)};
        }

        Response response;
        if (!response.ParseFromArray(response_str_.data(), response_len)) {
            co_return std::unexpected{make_rpc_error(RpcError::kSerializeFailed)};
        }
        co_return response;
    }

private:
    kosio::net::TcpStream stream_;
    std::string response_str_;
};
} // namespace foskv::rpc