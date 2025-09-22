#pragma once
#include <string_view>

namespace foskv::raft::detail {
constexpr std::string_view FIRST_INDEX_KEY = "first_index";
constexpr std::string_view LAST_INDEX_KEY = "last_index";
constexpr std::string_view PERSIST_STATE_KEY = "persist_state";
constexpr std::string_view SNAPSHOT_PATH = "member/snap";
constexpr std::string_view USER_DATA_PATH{"member/snap/db"};
constexpr std::string_view RAFT_LOG_PATH{"member/rocksdb/raft_logs"};
} // namespace foskv::raft::detail