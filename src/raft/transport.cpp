#include "foskv/raft/transport.hpp"

foskv::raft::Transport::Transport(std::string_view host, uint16_t port)
    : rpc_provider_(host, port) {
    // TODO: Load peers from file
}

foskv::raft::Transport::Transport(std::string_view host, uint16_t port, PeerMap &&peers)
    : rpc_provider_(host, port)
    , peers_(std::move(peers)) {}

auto foskv::raft::Transport::run() -> kosio::async::Task<> {
    std::size_t count{0};
    while (true) {
        auto ret = co_await rpc_provider_.run();
        if (!ret) [[unlikely]] {
            LOG_ERROR("{}", ret.error());
            count++;
        }
        if (count == 3) {
            LOG_ERROR("Failed to run after 3 attempts, exit.");
            break;
        }
    }
}
