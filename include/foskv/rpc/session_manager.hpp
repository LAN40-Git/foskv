#pragma once
#include "foskv/rpc/invoke_task.hpp"
#include <tbb/concurrent_hash_map.h>

namespace foskv::rpc {
class RpcProvider;
} // namespace foskv::rpc

namespace foskv::rpc::detail {
struct Session {
    explicit Session(uint64_t session_id, const kosio::net::SocketAddr& addr)
        : session_id(session_id), addr(addr) {}

    uint64_t                    session_id;
    kosio::net::SocketAddr      addr;
    util::SPSCQueue<InvokeTask> tasks;
};

class SessionManager {
    using SessionMap = tbb::concurrent_hash_map<uint64_t, std::shared_ptr<Session>>;
    friend class foskv::rpc::RpcProvider;
public:
    SessionManager() = default;

public:
    // Delete copy
    SessionManager(const SessionManager&) = delete;
    auto operator=(const SessionManager&) -> SessionManager& = delete;
    // Delete move
    SessionManager(SessionManager&&) = delete;
    auto operator=(SessionManager&&) -> SessionManager = delete;

public:
    /// @brief Assign a session
    /// @return A shared_ptr of session
    /// @note Keep the cite and remove the session when
    /// session closed, thread-safe
    auto assign(const kosio::net::SocketAddr& addr) -> std::shared_ptr<Session>;
    /// @brief Remove a session
    void remove(uint64_t session_id);

private:
    uint64_t   session_id_{0};
    SessionMap sessions_;
};
} // namespace foskv::rpc::detail