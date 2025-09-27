#include "foskv/rpc/provider.hpp"

auto foskv::rpc::RpcProvider::create(const kosio::net::SocketAddr& addr)
-> kosio::async::Task<Result<std::unique_ptr<RpcProvider>>> {
    auto has_listener = kosio::net::TcpListener::bind(addr);
    if (!has_listener) [[unlikely]] {
        LOG_ERROR("{}", has_listener.error());
        co_return std::unexpected{make_error(Error::kTcpListenerBindFailed)};
    }
    co_return std::make_unique<RpcProvider>(addr, std::move(has_listener.value()));
}

auto foskv::rpc::RpcProvider::run() -> kosio::async::Task<Result<void>> {
    {
        co_await mutex_.lock();
        std::lock_guard lock(mutex_, std::adopt_lock);
        is_running_.store(true, std::memory_order_relaxed);
    }
    LOG_VERBOSE("Listening on {}...", addr_);
    while (true) {
        auto has_stream = co_await listener_.accept();
        if (!has_stream) [[unlikely]] {
            LOG_VERBOSE("{}", has_stream.error());
            is_running_.store(false, std::memory_order_relaxed);
            co_return std::unexpected{make_error(Error::kTcpListenerAcceptFailed)};
        }
        auto& [stream, peer_addr] = has_stream.value();
        auto session = session_manager_.assign(std::move(stream), peer_addr);
        // LOG_INFO("Accept connection from {}, session {}", peer_addr, session->session_id);
        kosio::spawn(produce_invoke_tasks(session));
        kosio::spawn(consume_invoke_tasks(session));
    }
}

auto foskv::rpc::RpcProvider::shutdown() -> kosio::async::Task<void> {
    co_await mutex_.lock();
    std::lock_guard lock(mutex_, std::adopt_lock);
    if (auto ret = co_await listener_.close(); !ret) {
        LOG_ERROR("{}", ret.error());
        co_return;
    }

    // Close all sessions
    co_await session_manager_.shutdown();

    // while (is_running_.load(std::memory_order_relaxed)) {
    //     LOG_VERBOSE("Still listening on {}...", addr_);
    //     co_await kosio::time::sleep(50); // sleep for 50ms
    // }
    is_shutdown_.store(true, std::memory_order_relaxed);
}

void foskv::rpc::RpcProvider::register_invoke(
    ServiceType service_type,
    MethodType method_type,
    const detail::Invoke& invoke) {
    invokes_[service_type][method_type] = invoke;
}

auto foskv::rpc::RpcProvider::session_at(uint64_t session_id) const -> std::shared_ptr<detail::Session> {
    tbb::concurrent_hash_map<uint64_t, std::shared_ptr<detail::Session>>::const_accessor acc;
    if (session_manager_.sessions_.find(acc, session_id)) {
        return acc->second;
    }
    return nullptr;
}

auto foskv::rpc::RpcProvider::produce_invoke_tasks(std::shared_ptr<detail::Session> session)
-> kosio::async::Task<> {
    auto& tasks = session->tasks;
    auto& stream = session->stream;
    std::vector<char> buffer(detail::MAX_RPC_MESSAGE_SIZE);
    while (true) {
        // Recv fixed request header
        detail::FixedRequestHeader fixed_header;
        auto ret = co_await stream.read_exact(
            {reinterpret_cast<char*>(&fixed_header), sizeof(fixed_header)});
        if (!ret) [[unlikely]] {
            LOG_VERBOSE("{}", ret.error());
            break;
        }

        auto request_id = be64toh(fixed_header.request_id);
        auto service_type = fixed_header.service_type;
        auto method_type = fixed_header.method_type;
        auto payload_size = be32toh(fixed_header.payload_size);
        if (payload_size > detail::MAX_RPC_MESSAGE_SIZE) [[unlikely]] {
            LOG_ERROR("Message too large {}", payload_size);
            break;
        }

        // Recv req_payload
        ret = co_await stream.read_exact({buffer.data(), payload_size});
        if (!ret) [[unlikely]] {
            LOG_VERBOSE("{}", ret.error());
            break;
        }

        // Get invoke
        auto service = invokes_.find(service_type);
        if (service == invokes_.end()) [[unlikely]] {
            LOG_ERROR("Failed to find service for {}", RpcType::to_string(service_type));
            continue;
        }

        auto invoke = service->second.find(method_type);
        if (invoke == service->second.end()) [[unlikely]] {
            LOG_ERROR("Failed to find invoke for {}", RpcType::to_string(method_type));
            continue;
        }

        detail::InvokeTask task;
        task.request_id_ = request_id;
        task.req_payload_ = std::string{buffer.data(), payload_size};
        task.invoke_ = invoke->second;

        co_await tasks.push(std::move(task));
    }
    co_await tasks.shutdown();
    session_manager_.remove(session->session_id);
    LOG_VERBOSE("Session {} from {} : reader closed", session->session_id, session->addr);
}

auto foskv::rpc::RpcProvider::consume_invoke_tasks(std::shared_ptr<detail::Session> session)
-> kosio::async::Task<> {
    auto& tasks = session->tasks;
    auto& stream = session->stream;
    std::vector<char> buffer(detail::MAX_RPC_MESSAGE_SIZE);
    while (true) {
        auto has_task = co_await tasks.pop();
        if (!has_task) [[unlikely]] {
            LOG_ERROR("{}", has_task.error());
            break;
        }
        auto task = std::move(has_task.value());

        auto has_resp_payload = co_await task.invoke_(task.req_payload_,
                                                     {buffer.data(), buffer.capacity()},
                                                     session->session_id,
                                                     task.request_id_);
        if (!has_resp_payload) [[unlikely]] {
            LOG_ERROR("Failed to get resp payload from invoke : {}", has_resp_payload.error());
            continue;
        }

        auto resp_payload_size = has_resp_payload.value();
        // It may be an empty reply because the client request needs to
        // be wrapped in a log and synchronized
        if (resp_payload_size == 0 || resp_payload_size > detail::MAX_RPC_MESSAGE_SIZE) {
            continue;
        }

        detail::FixedResponseHeader fixed_header;
        fixed_header.request_id = htobe64(task.request_id_);
        fixed_header.payload_size = htobe32(resp_payload_size);

        // Send [rpc header size -> rpc header -> resp_payload]
        auto ret = co_await stream.write_vectored(
            std::span<const char>(reinterpret_cast<char*>(&fixed_header), sizeof(detail::FixedResponseHeader)),
            std::span<const char>(buffer.data(), resp_payload_size)
        );

        if (!ret) [[unlikely]] {
            LOG_ERROR("{}", ret.error());
            break;
        }
    }
    session_manager_.remove(session->session_id);
    LOG_VERBOSE("Session {} from {} : writer closed", session->session_id, session->addr);
}
