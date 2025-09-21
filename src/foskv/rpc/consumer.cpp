#include "foskv/rpc/consumer.hpp"

auto foskv::rpc::RpcConsumer::create(std::string_view host, uint16_t port)
-> kosio::async::Task<Result<std::unique_ptr<RpcConsumer>>> {
    auto has_addr = kosio::net::SocketAddr::parse(host, port);
    if (!has_addr) {
        co_return std::unexpected{make_error(Error::kInvalidRpcServerAddress)};
    }
    auto consumer = std::make_unique<RpcConsumer>(has_addr.value());
    co_return std::move(consumer);
}

auto foskv::rpc::RpcConsumer::call(
    std::string_view service_name,
    std::string_view method_name,
    std::string_view req_payload,
    detail::RpcCallback&& callback) -> kosio::async::Task<Result<void>> {
    detail::CallTask task{
        std::string{service_name},
        std::string{method_name},
        std::string{req_payload},
        std::move(callback)};
    co_await tasks_.push(std::move(task));

    // Check if reconnection is required
    if (fd_.load(std::memory_order_acquire) == -1) {
        co_return co_await this->connect();
    }

    co_return Result<void>{};
}

auto foskv::rpc::RpcConsumer::call(std::string &&service_name, std::string &&method_name, std::string &&req_payload,
    detail::RpcCallback &&callback) -> kosio::async::Task<Result<void>> {
    detail::CallTask task{
        std::move(service_name),
        std::move(method_name),
        std::move(req_payload),
        std::move(callback)};
    co_await tasks_.push(std::move(task));

    // Check if reconnection is required
    if (fd_.load(std::memory_order_acquire) == -1) {
        co_return co_await this->connect();
    }

    co_return Result<void>{};
}

auto foskv::rpc::RpcConsumer::shutdown() -> kosio::async::Task<> {
    is_shutdown_.store(true, std::memory_order_release);
    if (fd_.load(std::memory_order_acquire) != -1) {
        auto ret = co_await kosio::io::close(fd_);
        if (!ret) {
            LOG_ERROR("Failed to close consumer async : {}", ret.error());
            ::close(fd_);
        }
    }
    // If `is_producing_` or `is_consuming_` is still `true`, they will never
    // be `false` until they both exit.
    // If `is_producing_` or `is_consuming_` is `false`, it must be set before here
    // we check them, so just count down for it
    if (!is_producing_.load(std::memory_order_acquire)) {
        latch_.count_down();
    }
    if (!is_consuming_.load(std::memory_order_acquire)) {
        latch_.count_down();
    }
    // Wait for both exit
    co_await latch_.wait();
}

auto foskv::rpc::RpcConsumer::connect() -> kosio::async::Task<Result<void>> {
    auto has_stream = co_await kosio::net::TcpStream::connect(server_addr_);
    if (!has_stream) {
        LOG_ERROR("{}", has_stream.error());
        co_return std::unexpected{make_error(Error::kConnectRpcServerFailed)};
    }

    // Start produce and consume
    fd_.store(has_stream.value().fd(), std::memory_order_release);
    auto [reader, writer] = has_stream.value().into_split();
    is_producing_.store(true, std::memory_order_release);
    is_consuming_.store(true, std::memory_order_release);
    kosio::spawn(produce_callbacks(std::move(writer)));
    kosio::spawn(consume_callbacks(std::move(reader)));
    co_return Result<void>{};
}

auto foskv::rpc::RpcConsumer::produce_callbacks(kosio::net::OwnedTcpStreamWriter writer) -> kosio::async::Task<> {
    std::array<char, detail::MAX_RPC_MESSAGE_SIZE> buffer{};
    while (true) {
        auto task = co_await tasks_.pop();
        // Make rpc header
        RpcHeader rpc_header;
        rpc_header.set_request_id(request_id_);
        rpc_header.set_allocated_service_name(&task.service_name_);
        rpc_header.set_allocated_method_name(&task.method_name_);
        rpc_header.set_payload_size(task.req_payload_.size());

        // Send [rpc header size -> rpc header -> request payload]
        auto rpc_header_size = rpc_header.ByteSizeLong();
        if (rpc_header_size > buffer.max_size()) [[unlikely]] {
            continue;
        }
        rpc_header.SerializeToArray(buffer.data(), static_cast<int>(rpc_header_size));
        uint32_t rpc_header_size_net = htonl(static_cast<uint32_t>(rpc_header_size));

        // Although it is not possible, the first insertion here is to
        // avoid receiving a reply and the callback has not been inserted yet.
        callbacks_[request_id_] = std::move(task.callback_);

        auto ret = co_await writer.write_vectored(
            std::span<const char>(reinterpret_cast<char*>(&rpc_header_size_net), sizeof(uint32_t)),
            std::span<const char>(buffer.data(), rpc_header_size),
            std::span<const char>(task.req_payload_.data(), task.req_payload_.size())
        );

        if (!ret) [[unlikely]] {
            callbacks_.erase(request_id_);
            break;
        }

        // It only increments when the request is successfully sent.
        request_id_ += 1;
    }
    if (is_shutdown_.load(std::memory_order_acquire)) {
        co_await latch_.arrive_and_wait();
    }
    is_producing_.store(false, std::memory_order_release);
    if (!is_consuming_.load(std::memory_order_acquire)) {
        fd_.store(-1, std::memory_order_release);
    }
}

auto foskv::rpc::RpcConsumer::consume_callbacks(kosio::net::OwnedTcpStreamReader reader) -> kosio::async::Task<> {
    std::array<char, detail::MAX_RPC_MESSAGE_SIZE> buffer{};
    while (true) {
        // Recv rpc header size
        uint32_t rpc_header_size_net;
        auto ret = co_await reader.read_exact(
            {reinterpret_cast<char*>(&rpc_header_size_net), sizeof(uint32_t)});
        if (!ret) [[unlikely]] {
            LOG_ERROR("{}", ret.error());
            break;
        }

        uint32_t rpc_header_size = ntohl(rpc_header_size_net);
        if (rpc_header_size > buffer.max_size()) [[unlikely]] {
            LOG_ERROR("Response header too large.");
            break;
        }

        // Recv response header
        ret = co_await reader.read_exact(
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
        if (payload_size > buffer.max_size()) [[unlikely]] {
            LOG_ERROR("Response payload too large.");
            callbacks_.erase(request_id);
            break;
        }

        // Recv response payload
        ret = co_await reader.read_exact(
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
    if (is_shutdown_.load(std::memory_order_acquire)) {
        co_await latch_.arrive_and_wait();
    }
    is_consuming_.store(false, std::memory_order_release);
    if (!is_producing_.load(std::memory_order_acquire)) {
        fd_.store(-1, std::memory_order_release);
    }
}
