#include "foskv/rpc/provider.hpp"

auto foskv::rpc::RpcProvider::run() -> kosio::async::Task<Result<void>> {
    auto has_listener = kosio::net::TcpListener::bind(addr_);
    if (!has_listener) [[unlikely]] {
        LOG_ERROR("{}", has_listener.error());
        co_return std::unexpected{make_error(Error::kTcpListenerBindFailed)};
    }
    auto listener = std::move(has_listener.value());
    LOG_VERBOSE("Listening on {}...", addr_);
    while (true) {
        auto has_stream = co_await listener.accept();
        if (!has_stream) [[unlikely]] {
            LOG_ERROR("{}", has_stream.error());
            co_return std::unexpected{make_error(Error::kTcpStreamAcceptFailed)};
        }
        auto& [stream, peer_addr] = has_stream.value();
        LOG_VERBOSE("Accept connection from {}", peer_addr);
        auto session = session_manager_.assign(peer_addr);
        auto [owned_reader, owned_writer] = stream.into_split();
        kosio::spawn(produce_invoke_tasks(std::move(owned_reader), session));
        kosio::spawn(consume_invoke_tasks(std::move(owned_writer), session));
    }
}

void foskv::rpc::RpcProvider::register_invoke(
    ServiceType service_type,
    MethodType method_type,
    detail::Invoke&& invoke) {
    invokes_[service_type][method_type] = std::move(invoke);
}

auto foskv::rpc::RpcProvider::session_at(uint64_t session_id) const -> std::shared_ptr<detail::Session> {
    tbb::concurrent_hash_map<uint64_t, std::shared_ptr<detail::Session>>::const_accessor acc;
    if (session_manager_.sessions_.find(acc, session_id)) {
        return acc->second;
    }
    return nullptr;
}

auto foskv::rpc::RpcProvider::produce_invoke_tasks(
    kosio::net::OwnedTcpStreamReader reader, std::shared_ptr<detail::Session> session)
-> kosio::async::Task<> {
    auto& tasks = session->tasks;
    std::vector<char> buffer(detail::MAX_RPC_MESSAGE_SIZE);
    while (true) {
        // Recv fixed request header
        detail::FixedRequestHeader fixed_header;
        auto ret = co_await reader.read_exact(
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
        ret = co_await reader.read_exact({buffer.data(), payload_size});
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

auto foskv::rpc::RpcProvider::consume_invoke_tasks(
    kosio::net::OwnedTcpStreamWriter writer, std::shared_ptr<detail::Session> session)
-> kosio::async::Task<> {
    auto& tasks = session->tasks;
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
        auto ret = co_await writer.write_vectored(
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
