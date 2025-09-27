#include "foskv/rpc/session_manager.hpp"

#include <ranges>

auto foskv::rpc::detail::SessionManager::assign(kosio::net::TcpStream stream, const kosio::net::SocketAddr& addr)
-> std::shared_ptr<Session> {
    uint64_t session_id = session_id_++;
    auto session = std::make_shared<Session>(session_id, std::move(stream), addr);
    sessions_.emplace(session_id, session);
    return session;
}

void foskv::rpc::detail::SessionManager::remove(uint64_t session_id) {
    sessions_.erase(session_id);
}

auto foskv::rpc::detail::SessionManager::shutdown() -> kosio::async::Task<void> {
    for (auto &session: sessions_ | std::views::values) {
        auto ret = co_await session->stream.close();
        if (!ret) {
            LOG_ERROR("{}", ret.error());
        }
    }
    sessions_.clear();
}
