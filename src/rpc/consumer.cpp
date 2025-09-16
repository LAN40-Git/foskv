#include "foskv/rpc/consumer.hpp"

auto foskv::rpc::RpcConsumer::connect(std::string_view host, uint16_t port)
-> kosio::async::Task<RpcResult<std::unique_ptr<RpcConsumer>>> {
    auto has_addr = kosio::net::SocketAddr::parse(host, port);
    if (!has_addr) [[unlikely]] {
        co_return std::unexpected{make_rpc_error(RpcError::kConnectFailed)};
    }
    auto has_stream = co_await kosio::net::TcpStream::connect(has_addr.value());
    if (!has_stream) [[unlikely]] {
        co_return std::unexpected{make_rpc_error(RpcError::kConnectFailed)};
    }
    co_return std::make_unique<RpcConsumer>(std::move(has_stream.value()));
}

auto foskv::rpc::RpcConsumer::call(std::string &&service_name, std::string &&method_name, std::string &&payload,
    detail::RpcCallback&& callback) -> kosio::async::Task<RpcResult<void>> {
    co_await mutex_.lock();
    std::lock_guard lock(mutex_, std::adopt_lock);
    // Make request header
    RequestHeader req_header;
    req_header.set_request_id(request_id_);
    req_header.set_service_name(std::move(service_name));
    req_header.set_method_name(std::move(method_name));
    req_header.set_payload_size(payload.size());

    // Send [request header size -> request header -> request payload]
    auto req_header_size = req_header.ByteSizeLong();
    if (req_header_size > buffer_.capacity()) [[unlikely]] {
        co_return std::unexpected{make_rpc_error(RpcError::kMessageTooLarge)};
    }
    req_header.SerializeToArray(buffer_.data(), static_cast<int>(req_header_size));
    uint32_t req_header_size_net = htonl(static_cast<uint32_t>(req_header_size));

    // Although it is not possible, the first insertion here is to
    // avoid receiving a reply and the callback has not been inserted yet.
    callbacks_.emplace(request_id_, std::move(callback));

    auto ret = co_await stream_.write_vectored(
        std::span<const char>(reinterpret_cast<char*>(&req_header_size_net), sizeof(uint32_t)),
        std::span<const char>(buffer_.data(), req_header_size),
        std::span<const char>(payload.data(), payload.size())
    );

    if (!ret) [[unlikely]] {
        co_return std::unexpected{make_rpc_error(RpcError::kSendFailed)};
    }

    // It only increments when the request is successfully sent.
    request_id_ += 1;
    co_return RpcResult<void>{};
}

auto foskv::rpc::RpcConsumer::reconnect() -> kosio::async::Task<bool> {
    auto ret = co_await kosio::net::TcpStream::connect(server_addr_);
    if (!ret) [[unlikely]] {
        LOG_ERROR("Failed to reconnect to the server {}.", server_addr_);
        co_return false;
    }
    // The old stream will close automatically
    stream_ = std::move(ret.value());
    co_return true;
}

auto foskv::rpc::RpcConsumer::run() -> kosio::async::Task<> {
    std::vector<char> buffer(4 * 1024 * 1024); // 4MB
    // Break if failed to reconnect to the rpc server or receive invalid message
    while (true) {
        // Recv response header size
        uint32_t resp_header_size_net;
        auto ret = co_await stream_.read_exact(
            {reinterpret_cast<char*>(&resp_header_size_net), sizeof(uint32_t)});
        if (!ret && !co_await reconnect()) [[unlikely]] {
            LOG_ERROR("{}", ret.error());
            break;
        }

        uint32_t resp_header_size = ntohl(resp_header_size_net);
        if (resp_header_size > buffer.capacity()) [[unlikely]] {
            LOG_ERROR("Response header too large.");
            break;
        }

        // Recv response header
        ret = co_await stream_.read_exact(
            {buffer.data(), resp_header_size});
        if (!ret && !co_await reconnect()) [[unlikely]] {
            LOG_ERROR("{}", ret.error());
            break;
        }

        ResponseHeader header;
        if (!header.ParseFromArray(buffer.data(), static_cast<int>(resp_header_size))) {
            LOG_ERROR("Failed to parse response header.");
            break;
        }
        auto request_id = header.request_id();
        auto payload_size = header.payload_size();
        if (payload_size > buffer.capacity()) [[unlikely]] {
            LOG_ERROR("Response payload too large.");
            break;
        }

        // Recv response payload
        ret = co_await stream_.read_exact(
            {buffer.data(), payload_size});
        if (!ret && !co_await reconnect()) [[unlikely]] {
            LOG_ERROR("{}", ret.error());
            break;
        }

        if (callbacks_.contains(request_id)) {
            co_await callbacks_[request_id](std::string_view{buffer.data(), payload_size});
            // Since request_id is monotonically incrementing, it is thread safe here.
            callbacks_.unsafe_erase(request_id);
        }
    }
    latch_.count_down();
}

auto foskv::rpc::RpcConsumer::close() -> kosio::async::Task<> {
    co_await stream_.close();
    co_await latch_.wait();
}
