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
    std::string_view service_name,
    std::string_view method_name,
    detail::Invoke&& invoke) {
    invokes_[service_name][method_name] = std::move(invoke);
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
    while (true) {
        // Recv rpc header size
        uint32_t rpc_header_size_net;
        auto recv_ret = co_await reader.read_exact(
            {reinterpret_cast<char*>(&rpc_header_size_net), sizeof(uint32_t)});
        if (!recv_ret) [[unlikely]] {
            LOG_ERROR("{}", recv_ret.error());
            break;
        }

        uint32_t rpc_header_size = ntohl(rpc_header_size_net);
        if (rpc_header_size > detail::MAX_RPC_MESSAGE_SIZE) [[unlikely]] {
            LOG_ERROR("Message too large.", rpc_header_size);
            break;
        }

        // TODO: Use buffer pools
        detail::InvokeTask task;
        task.req_payload_.resize(rpc_header_size);

        // Recv rpc header
        recv_ret = co_await reader.read_exact(task.req_payload_);
        if (!recv_ret) [[unlikely]] {
            LOG_ERROR("{}", recv_ret.error());
            break;
        }

        // Parse rpc header
        RpcHeader rpc_header;
        if (!rpc_header.ParseFromString(task.req_payload_)) {
            LOG_ERROR("Failed to parse rpc header");
            break;
        }

        auto request_id = rpc_header.request_id();
        auto service_name = rpc_header.service_name();
        auto method_name = rpc_header.method_name();
        auto req_payload_size = rpc_header.payload_size();

        // TODO: Use buffer pools
        task.request_id_ = request_id;
        task.req_payload_.resize(req_payload_size);

        // Recv request payload
        recv_ret = co_await reader.read_exact(task.req_payload_);
        if (!recv_ret) [[unlikely]] {
            LOG_ERROR("{}", recv_ret.error());
            break;
        }

        // Get invoke
        auto service = invokes_.find(service_name);
        if (service == invokes_.end()) [[unlikely]] {
            LOG_ERROR("Failed to find service for {}", service_name);
        }

        auto invoke = service->second.find(method_name);
        if (invoke == service->second.end()) [[unlikely]] {
            LOG_ERROR("Failed to find invoke for {}", method_name);
        }

        task.invoke_ = invoke->second;
        co_await tasks.push(std::move(task));
    }
    session_manager_.remove(session->session_id);
    LOG_VERBOSE("Session {} from {} : reader closed", session->session_id, session->addr);
}

auto foskv::rpc::RpcProvider::consume_invoke_tasks(
    kosio::net::OwnedTcpStreamWriter writer, std::shared_ptr<detail::Session> session)
-> kosio::async::Task<> {
    auto& tasks = session->tasks;
    std::array<char, sizeof(RpcHeader)> buffer{};
    std::array<char, detail::MAX_RPC_MESSAGE_SIZE> resp_buffer{};
    while (true) {
        auto task = co_await tasks.pop();

        auto has_resp_payload = co_await task.invoke_(task.req_payload_,
                                                     {resp_buffer.data(), resp_buffer.max_size()},
                                                     session->session_id,
                                                     task.request_id_);
        if (!has_resp_payload) [[unlikely]] {
            LOG_ERROR("Failed to get resp payload from invoke : {}", has_resp_payload.error());
            continue;
        }

        auto resp_payload_size = has_resp_payload.value();
        // It may be an empty reply because the client request needs to
        // be wrapped in a log and synchronized
        if (resp_payload_size == 0) {
            continue;
        }

        // Make rpc header
        RpcHeader rpc_header;
        rpc_header.set_request_id(task.request_id_);
        rpc_header.set_payload_size(resp_payload_size);
        auto rpc_header_size = rpc_header.ByteSizeLong();
        if (!rpc_header.SerializeToArray(buffer.data(), rpc_header_size)) [[unlikely]] {
            LOG_ERROR("Failed to serialize response.");
            continue;
        }

        // Send [rpc header size -> rpc header -> resp_payload]
        auto rpc_header_size_net = htonl(rpc_header_size);

        auto ret = co_await writer.write_vectored(
            std::span<const char>(reinterpret_cast<char*>(&rpc_header_size_net), sizeof(uint32_t)),
            std::span<const char>(buffer.data(), rpc_header_size),
            std::span<const char>(resp_buffer.data(), resp_payload_size)
        );

        if (!ret) [[unlikely]] {
            LOG_ERROR("{}", ret.error());
            break;
        }
    }
    session_manager_.remove(session->session_id);
    LOG_VERBOSE("Session {} from {} : write closed", session->session_id, session->addr);
}
