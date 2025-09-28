#include "foskv/rpc/consumer.hpp"

auto foskv::rpc::RpcConsumer::create(std::string_view host, uint16_t port)
-> kosio::async::Task<Result<std::unique_ptr<RpcConsumer>>> {
    auto has_addr = kosio::net::SocketAddr::parse(host, port);
    if (!has_addr) {
        LOG_ERROR("{}", has_addr.error());
        co_return std::unexpected{make_error(Error::kInvalidRpcServerAddress)};
    }
    co_return std::make_unique<RpcConsumer>(has_addr.value(),kosio::net::TcpStream{kosio::net::detail::Socket{-1}});
}

auto foskv::rpc::RpcConsumer::call(
    ServiceType service_type,
    MethodType method_type,
    std::string_view req_payload,
    const RpcCallback &callback) -> kosio::async::Task<Result<void>> {
    co_await mutex_.lock();
    std::lock_guard lock(mutex_, std::adopt_lock);

    if (!stream_.is_valid()) {
        if (auto ret = co_await this->connect(); !ret) {
            co_return std::unexpected{ret.error()};
        }
    }

    // Make fixed header
    detail::FixedRequestHeader fixed_header;
    fixed_header.request_id = htobe64(request_id_);
    fixed_header.service_type = service_type;
    fixed_header.method_type = method_type;
    fixed_header.payload_size = htobe32(req_payload.size());

    // Although it is not possible, the first insertion here is to
    // avoid receiving a reply and the callback has not been inserted yet.
    callbacks_.emplace(request_id_++, callback);

    auto ret = co_await stream_.write_vectored(
        std::span<const char>(reinterpret_cast<char*>(&fixed_header), sizeof(fixed_header)),
        std::span<const char>(req_payload.data(), req_payload.size())
    );

    if (!ret) {
        LOG_ERROR("{}", ret.error());
        co_await stream_.close();
        co_return std::unexpected{make_error(Error::kCallRpcFailed)};
    }

    co_return Result<void>{};
}

auto foskv::rpc::RpcConsumer::call(
    ServiceType service_type,
    MethodType method_type,
    std::string_view req_payload,
    RpcCallback&& callback) -> kosio::async::Task<Result<void>> {
    co_await mutex_.lock();
    std::lock_guard lock(mutex_, std::adopt_lock);

    if (!stream_.is_valid()) {
        if (auto ret = co_await this->connect(); !ret) {
            co_return std::unexpected{ret.error()};
        }
    }

    // Make fixed header
    detail::FixedRequestHeader fixed_header;
    fixed_header.request_id = htobe64(request_id_);
    fixed_header.service_type = service_type;
    fixed_header.method_type = method_type;
    fixed_header.payload_size = htobe32(req_payload.size());

    // Although it is not possible, the first insertion here is to
    // avoid receiving a reply and the callback has not been inserted yet.
    callbacks_.emplace(request_id_++, std::move(callback));

    auto ret = co_await stream_.write_vectored(
        std::span<const char>(reinterpret_cast<char*>(&fixed_header), sizeof(fixed_header)),
        std::span<const char>(req_payload.data(), req_payload.size())
    );

    if (!ret) {
        LOG_ERROR("{}", ret.error());
        co_await stream_.close();
        co_return std::unexpected{make_error(Error::kCallRpcFailed)};
    }

    co_return Result<void>{};
}

auto foskv::rpc::RpcConsumer::shutdown() -> kosio::async::Task<> {
    co_await mutex_.lock();
    std::lock_guard lock(mutex_, std::adopt_lock);

    if (is_shutdown_.load(std::memory_order_relaxed)) {
        co_return;
    }
    is_shutdown_.store(true, std::memory_order_relaxed);

    if (stream_.is_valid()) {
        while (auto ret = co_await kosio::io::cancel(stream_.fd(), IORING_ASYNC_CANCEL_FD)) {}
        if (auto ret = co_await stream_.close(); !ret) {
            LOG_ERROR("{}", ret.error());
        }
    }

    while (is_running_.load(std::memory_order_relaxed)) {
        // LOG_VERBOSE("Still running, wating for 50ms...");
        co_await kosio::time::sleep(50);
    }
}

void foskv::rpc::RpcConsumer::run() {
    is_running_.store(true, std::memory_order_relaxed);
    kosio::spawn(consume_callbacks());
}

auto foskv::rpc::RpcConsumer::connect() -> kosio::async::Task<Result<void>> {
    /* Hold mutex_ */
    if (stream_.is_valid() || is_running_.load(std::memory_order_relaxed)) {
        co_return Result<void>{};
    }
    request_id_ = 0;

    auto has_stream = co_await kosio::net::TcpStream::connect(server_addr_);
    if (!has_stream) {
        LOG_VERBOSE("{}", has_stream.error());
        co_return std::unexpected{make_error(Error::kConnectRpcServerFailed)};
    }

    LOG_VERBOSE("Connect to {}", server_addr_);

    // Disable Nagle
    auto has_disable_nagle = has_stream.value().set_nodelay(true);
    if (!has_disable_nagle) {
        LOG_ERROR("Failed to disable nagle : {}", has_disable_nagle.error());
    }

    stream_ = std::move(has_stream.value());
    this->run();
    co_return Result<void>{};
}

auto foskv::rpc::RpcConsumer::consume_callbacks() -> kosio::async::Task<> {
    std::vector<char> buffer(detail::MAX_RPC_MESSAGE_SIZE);
    while (true) {
        // Recv fixed response header
        detail::FixedResponseHeader fixed_header;
        auto ret = co_await stream_.read_exact(
            {reinterpret_cast<char*>(&fixed_header), sizeof(detail::FixedResponseHeader)});
        if (!ret) [[unlikely]] {
            LOG_ERROR("{}", ret.error());
            break;
        }

        auto request_id = be64toh(fixed_header.request_id);
        auto payload_size = be32toh(fixed_header.payload_size);
        if (payload_size > detail::MAX_RPC_MESSAGE_SIZE) [[unlikely]] {
            LOG_ERROR("Response payload too large, request_id : {}, payload_size : {}.", request_id, payload_size);
            callbacks_.erase(request_id);
            break;
        }

        // Recv resp_payload
        ret = co_await stream_.read_exact({buffer.data(), payload_size});
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
    is_running_.store(false, std::memory_order_relaxed);
}