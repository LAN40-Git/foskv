#include "foskv/rpc/consumer.hpp"

foskv::rpc::RpcConsumer::RpcConsumer(kosio::net::TcpStream &&stream,
                                     const kosio::net::SocketAddr& server_addr)
    : buffer_(detail::MAX_RPC_MESSAGE_SIZE)
    , stream_(std::move(stream))
    , server_addr_(server_addr) {
    LOG_INFO("Connect to {}", server_addr_);
    kosio::spawn(run());
}

foskv::rpc::RpcConsumer::~RpcConsumer() {
    assert(!is_running_.load(std::memory_order_acquire));
    assert(is_shutdown_.load(std::memory_order_acquire));
}

auto foskv::rpc::RpcConsumer::connect(const kosio::net::SocketAddr& server_addr)
-> kosio::async::Task<kosio::Result<std::unique_ptr<RpcConsumer>>> {
    auto has_stream = co_await kosio::net::TcpStream::connect(server_addr);
    if (!has_stream) [[unlikely]] {
        co_return std::unexpected{has_stream.error()};
    }
    co_return std::make_unique<RpcConsumer>(std::move(has_stream.value()), server_addr);
}

auto foskv::rpc::RpcConsumer::call(
    std::string_view service_name,
    std::string_view method_name,
    std::string_view payload,
    RpcCallback&& callback) -> kosio::async::Task<RpcResult<void>> {
    co_await mutex_.lock();
    std::lock_guard lock(mutex_, std::adopt_lock);
    // Make request header
    RequestHeader req_header;
    req_header.set_request_id(request_id_);
    req_header.set_service_name({service_name.data(), service_name.size()});
    req_header.set_method_name({method_name.data(), method_name.size()});
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
        co_await reconnect();
        co_return std::unexpected{make_rpc_error(RpcError::kSendFailed)};
    }

    // It only increments when the request is successfully sent.
    request_id_ += 1;
    co_return RpcResult<void>{};
}

auto foskv::rpc::RpcConsumer::shutdown() -> kosio::async::Task<> {
    auto ret = co_await stream_.shutdown(SHUT_RDWR);
    if (!ret) [[unlikely]] {
        LOG_ERROR("Failed to shutdown consumer : {}", ret.error());
        co_return;
    }
    is_shutdown_.store(true, std::memory_order_release);
    if (is_running_.load(std::memory_order_acquire)) {
        LOG_VERBOSE("Consumer shutting down...");
        co_await latch_.wait();
    }
}

auto foskv::rpc::RpcConsumer::run() -> kosio::async::Task<> {
    if (is_shutdown_.load(std::memory_order_acquire) ||
        is_running_.load(std::memory_order_acquire)) {
        co_return;
    }
    is_running_.store(true, std::memory_order_release);
    std::vector<char> buffer(4 * 1024 * 1024); // 4MB
    // Break if failed to reconnect to the rpc server or receive invalid message
    while (true) {
        // Recv response header size
        uint32_t resp_header_size_net;
        auto ret = co_await stream_.read_exact(
            {reinterpret_cast<char*>(&resp_header_size_net), sizeof(uint32_t)});
        if (!ret) [[unlikely]] {
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
        if (!ret) [[unlikely]] {
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
            callbacks_.erase(request_id);
            break;
        }

        // Recv response payload
        ret = co_await stream_.read_exact(
            {buffer.data(), payload_size});
        if (!ret) [[unlikely]] {
            LOG_ERROR("{}", ret.error());
            callbacks_.erase(request_id);
            break;
        }

        if (callbacks_.contains(request_id)) {
            co_await callbacks_[request_id](std::string_view{buffer.data(), payload_size});
            // Since request_id is monotonically
            // incrementing, it is thread safe here.
            callbacks_.erase(request_id);
        }
    }
    is_running_.store(false, std::memory_order_release);
    if (is_shutdown_.load(std::memory_order_acquire)) {
        latch_.count_down();
    }
}

auto foskv::rpc::RpcConsumer::reconnect() -> kosio::async::Task<RpcResult<void>> {
    if (is_shutdown_.load(std::memory_order_acquire) ||
        is_running_.load(std::memory_order_acquire)) {
            co_return std::unexpected{make_rpc_error(RpcError::kReconnectFailed)};
    }
    auto ret = co_await kosio::net::TcpStream::connect(server_addr_);
    if (!ret) {
        co_return std::unexpected{make_rpc_error(RpcError::kReconnectFailed)};
    }
    // The old stream has been closed or error,
    // so this will be ok
    stream_ = std::move(ret.value());
    kosio::spawn(run());
    co_return RpcResult<void>{};
}
