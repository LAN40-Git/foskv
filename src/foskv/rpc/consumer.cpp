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
    ServiceType service_type,
    MethodType method_type,
    std::string_view req_payload,
    RpcCallback&& callback) -> kosio::async::Task<Result<void>> {
    auto task = detail::CallTask{service_type, method_type, req_payload, std::move(callback)};
    co_await tasks_.push(std::move(task));

    // Check if reconnection is required
    if (fd_.load(std::memory_order_acquire) < 0) {
        co_await mutex_.lock();
        std::lock_guard lock(mutex_, std::adopt_lock);
        co_return co_await this->connect();
    }

    co_return Result<void>{};
}

auto foskv::rpc::RpcConsumer::call(ServiceType service_type, MethodType method_type,
    std::string&& req_payload, RpcCallback&& callback) -> kosio::async::Task<Result<void>> {
    auto task = detail::CallTask{service_type, method_type, std::move(req_payload), std::move(callback)};
    co_await tasks_.push(std::move(task));

    // Check if reconnection is required
    if (fd_.load(std::memory_order_acquire) < 0) {
        co_await mutex_.lock();
        std::lock_guard lock(mutex_, std::adopt_lock);
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
    if (fd_.load(std::memory_order_relaxed) >= 0) {
        co_return Result<void>{};
    }

    auto has_stream = co_await kosio::net::TcpStream::connect(server_addr_);
    if (!has_stream) {
        LOG_ERROR("{}", has_stream.error());
        co_return std::unexpected{make_error(Error::kConnectRpcServerFailed)};
    }

    // Start produce and consume
    fd_.store(has_stream.value().fd(), std::memory_order_release);
    // Disable Nagle
    auto has_disable_nagle = has_stream.value().set_nodelay(true);
    if (!has_disable_nagle) {
        LOG_ERROR("Failed to disable nagle : {}", has_disable_nagle.error());
    }
    auto [reader, writer] = has_stream.value().into_split();
    is_producing_.store(true, std::memory_order_relaxed);
    is_consuming_.store(true, std::memory_order_relaxed);
    kosio::spawn(produce_callbacks(std::move(writer)));
    kosio::spawn(consume_callbacks(std::move(reader)));
    co_return Result<void>{};
}

auto foskv::rpc::RpcConsumer::produce_callbacks(kosio::net::OwnedTcpStreamWriter writer) -> kosio::async::Task<> {
    std::vector<char> buffer(detail::MAX_RPC_MESSAGE_SIZE);
    while (true) {
        auto has_task = co_await tasks_.pop();
        if (!has_task) {
            LOG_ERROR("{}", has_task.error());
            break;
        }
        auto task = std::move(has_task.value());

        auto payload_size = task.req_payload_.size();
        if (payload_size > buffer.capacity()) {
            LOG_ERROR("Message too large : {}", payload_size);
            continue;
        }

        // Make fixed header
        detail::FixedRequestHeader fixed_header;
        fixed_header.request_id = htobe64(request_id_);
        fixed_header.service_type = task.service_type_;
        fixed_header.method_type = task.method_type_;
        fixed_header.payload_size = htobe32(payload_size);

        // Although it is not possible, the first insertion here is to
        // avoid receiving a reply and the callback has not been inserted yet.
        callbacks_.emplace(request_id_++, std::move(task.callback_));

        // Send [fixed header -> request payload]
        // char* ptr = buffer.data();
        // memcpy(ptr, &fixed_header, sizeof(fixed_header)); ptr += sizeof(fixed_header);
        // memcpy(ptr, task.req_payload_.data(), payload_size);
        // auto ret = co_await writer.write_all({buffer.data(), sizeof(fixed_header) + payload_size});
        //
        // if (!ret) {
        //     LOG_ERROR("{}", ret.error());
        //     break;
        // }

        /* Unsafe, do not use */
        auto ret = co_await writer.write_vectored(
            std::span<const char>(reinterpret_cast<char*>(&fixed_header), sizeof(fixed_header)),
            std::span<const char>(task.req_payload_.data(), task.req_payload_.size())
        );

        if (!ret) {
            LOG_ERROR("{}", ret.error());
            break;
        }
    }
    if (is_shutdown_.load(std::memory_order_acquire)) {
        co_await latch_.arrive_and_wait();
    }
    is_producing_.store(false, std::memory_order_release);
    if (!is_consuming_.load(std::memory_order_acquire)) {
        fd_.store(-1, std::memory_order_release);
        callbacks_.clear();
    }
}

auto foskv::rpc::RpcConsumer::consume_callbacks(kosio::net::OwnedTcpStreamReader reader) -> kosio::async::Task<> {
    std::vector<char> buffer(2 * detail::MAX_RPC_MESSAGE_SIZE);
    while (true) {
        // Recv fixed response header
        detail::FixedResponseHeader fixed_header;
        auto ret = co_await reader.read_exact(
            {reinterpret_cast<char*>(&fixed_header), sizeof(detail::FixedResponseHeader)});
        if (!ret) [[unlikely]] {
            LOG_ERROR("{}", ret.error());
            break;
        }

        auto request_id = be64toh(fixed_header.request_id);
        auto payload_size = be32toh(fixed_header.payload_size);
        if (payload_size > detail::MAX_RPC_MESSAGE_SIZE) [[unlikely]] {
            LOG_ERROR("Response header too large, payload_size : {}.", payload_size);
            callbacks_.erase(request_id);
            break;
        }

        // Recv resp_payload
        ret = co_await reader.read_exact({buffer.data(), payload_size});
        if (!ret) [[unlikely]] {
            LOG_ERROR("{}", ret.error());
            callbacks_.erase(request_id);
            break;
        }

        tbb::concurrent_hash_map<uint64_t, RpcCallback>::accessor acc;
        if (callbacks_.find(acc, request_id)) {
            auto callback = std::move(acc->second);
            acc.release();
            co_await callback(std::string_view{buffer.data(), payload_size});
            callbacks_.erase(request_id);
        }
    }
    co_await tasks_.shutdown();
    if (is_shutdown_.load(std::memory_order_acquire)) {
        co_await latch_.arrive_and_wait();
    }
    is_consuming_.store(false, std::memory_order_release);
    if (!is_producing_.load(std::memory_order_acquire)) {
        fd_.store(-1, std::memory_order_release);
        callbacks_.clear();
    }
}
