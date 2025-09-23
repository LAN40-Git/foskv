#include "foskv/rpc/session_manager.hpp"

auto foskv::rpc::detail::SessionManager::assign(const kosio::net::SocketAddr& addr)
-> std::shared_ptr<Session> {
    uint64_t session_id = session_id_++;
    auto session = std::make_shared<Session>(session_id, addr);
    sessions_.emplace(session_id, session);
    return session;
}

void foskv::rpc::detail::SessionManager::remove(uint64_t session_id) {
    sessions_.erase(session_id);
}
