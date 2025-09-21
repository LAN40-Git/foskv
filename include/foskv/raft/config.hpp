#pragma once
#include <string_view>

namespace foskv::raft::detail {
constexpr std::string_view PERSISTENT_KEY = "P";
constexpr std::string_view START_LOG_INDEX = "S";
constexpr std::string_view END_LOG_INDEX = "E";
constexpr std::string_view PERSISTENT_PATH{"member/persistent"};
constexpr std::string_view USER_DATA_PATH{"usr/data"};
constexpr std::string_view RAFT_LOG_PATH{"member/rocksdb/raft_logs"};
} // namespace foskv::raft::detail