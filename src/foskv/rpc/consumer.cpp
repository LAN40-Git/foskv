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

foskv::rpc::RpcConsumer::RpcConsumer(RpcConsumer&& other) noexcept
    : is_shutdown_(other.is_shutdown_.load(std::memory_order_relaxed))
    , is_running_(other.is_running_.load(std::memory_order_relaxed))
    , request_id_(other.request_id_)
    , buffer_(std::move(other.buffer_))
    , stream_(std::move(other.stream_))
    , server_addr_(other.server_addr_)
    , callbacks_(std::move(other.callbacks_)) {}

auto foskv::rpc::RpcConsumer::operator=(RpcConsumer&& other) noexcept -> RpcConsumer& {
    is_shutdown_.store(other.is_shutdown_.load(std::memory_order_relaxed));
    is_running_.store(other.is_running_.load(std::memory_order_relaxed));
    request_id_ = other.request_id_;
    buffer_ = std::move(other.buffer_);
    stream_ = std::move(other.stream_);
    server_addr_ = other.server_addr_;
    callbacks_ = std::move(other.callbacks_);
    return *this;
}

auto foskv::rpc::RpcConsumer::connect(std::string_view host, uint16_t port)
-> kosio::async::Task<kosio::Result<RpcConsumer>> {
    auto has_addr = kosio::net::SocketAddr::parse(host, port);
    if (!has_addr) {
        co_return std::unexpected{has_addr.error()};
    }
    auto has_stream = co_await kosio::net::TcpStream::connect(has_addr.value());
    if (!has_stream) [[unlikely]] {
        co_return std::unexpected{has_stream.error()};
    }
    co_return RpcConsumer{std::move(has_stream.value()), has_addr.value()};
}

auto foskv::rpc::RpcConsumer::call(
    std::string_view service_name,
    std::string_view method_name,
    std::string_view payload,
    RpcCallback&& callback) -> kosio::async::Task<RpcResult<void>> {
    co_await mutex_.lock();
    std::lock_guard lock(mutex_, std::adopt_lock);
    // Make rpc header
    RpcHeader rpc_header;
    rpc_header.set_request_id(request_id_);
    rpc_header.set_service_name({service_name.data(), service_name.size()});
    rpc_header.set_method_name({method_name.data(), method_name.size()});
    rpc_header.set_payload_size(payload.size());

    // Send [rpc header size -> rpc header -> request payload]
    auto rpc_header_size = rpc_header.ByteSizeLong();
    if (rpc_header_size > buffer_.capacity()) [[unlikely]] {
        co_return std::unexpected{make_rpc_error(RpcError::kMessageTooLarge)};
    }
    rpc_header.SerializeToArray(buffer_.data(), static_cast<int>(rpc_header_size));
    uint32_t rpc_header_size_net = htonl(static_cast<uint32_t>(rpc_header_size));

    // Although it is not possible, the first insertion here is to
    // avoid receiving a reply and the callback has not been inserted yet.
    callbacks_.emplace(request_id_, std::move(callback));

    auto ret = co_await stream_.write_vectored(
        std::span<const char>(reinterpret_cast<char*>(&rpc_header_size_net), sizeof(uint32_t)),
        std::span<const char>(buffer_.data(), rpc_header_size),
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
    std::vector<char> buffer(detail::MAX_RPC_MESSAGE_SIZE);
    // Break if failed to reconnect to the rpc server or receive invalid message
    while (true) {
        // Recv rpc header size
        uint32_t rpc_header_size_net;
        auto ret = co_await stream_.read_exact(
            {reinterpret_cast<char*>(&rpc_header_size_net), sizeof(uint32_t)});
        if (!ret) [[unlikely]] {
            LOG_ERROR("{}", ret.error());
            break;
        }

        uint32_t rpc_header_size = ntohl(rpc_header_size_net);
        if (rpc_header_size > buffer.capacity()) [[unlikely]] {
            LOG_ERROR("Response header too large.");
            break;
        }

        // Recv response header
        ret = co_await stream_.read_exact(
            {buffer.data(), rpc_header_size});
        if (!ret) [[unlikely]] {
            LOG_ERROR("{}", ret.error());
            break;
        }

        RpcHeader rpc_header;
        if (!rpc_header.ParseFromArray(buffer.data(), static_cast<int>(rpc_header_size))) {
            LOG_ERROR("Failed to parse response header.");
            break;
        }
        auto request_id = rpc_header.request_id();
        auto payload_size = rpc_header.payload_size();
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
