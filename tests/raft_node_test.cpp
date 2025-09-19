#include "foskv/raft/raft_node.hpp"

auto main() -> int {
    std::vector<int> arr{1, 2, 3, 4, 5, 6, 7, 8, 9};
    std::span<int> span{arr};
    for (auto& s : span) {
        std::cout << s << std::endl;
    }
    span = span.subspan(0, 3);
    for (auto& s : span) {
        std::cout << s << std::endl;
    }
}