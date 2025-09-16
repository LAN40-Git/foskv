#pragma once
#include <cstddef>

namespace foskv::rpc::detail {
constexpr std::size_t MAX_RPC_MESSAGE_SIZE = 4 * 1024 * 1024; // 4MB
} // namespace foskv::rpc::detail