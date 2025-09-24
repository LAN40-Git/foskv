#include <vector>
#include <algorithm>
#include <iostream>
#include <ostream>

auto main() -> int {
    std::vector<int> vec = {3, 4, 8, 1, 2, 9, 0};
    std::ranges::sort(vec);
    for (auto i : vec) {
        std::cout << i << std::endl;
    }
    return 0;
}
