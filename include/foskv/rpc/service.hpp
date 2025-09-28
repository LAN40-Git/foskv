#pragma once
#include "foskv/rpc/pb/rpc.pb.h"
#include "foskv/rpc/pb/kv.pb.h"
#include "foskv/rpc/pb/raft.pb.h"
#include <string_view>

namespace foskv::rpc {
enum class ServiceType : uint8_t {
    kRaft = 0,
    kKv,
};

enum class MethodType : uint8_t {
    kRaftRequestVote = 0,
    kRaftAppendEntries,
    kRaftInstallSnapshot,
    kKvPut,
    kKvGet,
    kKvDelete,
};
} // namespace foskv::rpc