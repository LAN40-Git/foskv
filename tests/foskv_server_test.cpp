#include "foskv/raft/raft_node.hpp"
#include "kosio/signal.hpp"


auto server() -> kosio::async::Task<> {

}

auto main() -> int {
    kosio::runtime::CurrentThreadBuilder::default_create().block_on(server());
}