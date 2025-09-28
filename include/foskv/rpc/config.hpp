#pragma once
#include <cstddef>

namespace foskv::rpc::detail {
constexpr std::size_t MAX_RPC_MESSAGE_SIZE = 4 * 1024 * 1024; // 4MB
constexpr std::size_t DEFAULT_CALLBACKS_HASH_SIZE = 4096;
} // namespace foskv::rpc::detail