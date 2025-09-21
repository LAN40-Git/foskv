#pragma once
#include "foskv/rpc/pb/kv.pb.h"
#include "foskv/rpc/pb/raft.pb.h"
#include <string_view>

namespace foskv::rpc {
class RaftService {
public:
#ifdef ENABLE_HUMAN_READABLE_RPC_NAMES
    static constexpr std::string_view ServiceName = "Raft";
    static constexpr std::string_view RequestVote = "RequestVote";
    static constexpr std::string_view AppendEntries = "AppendEntries";
    static constexpr std::string_view InstallSnapshot = "InstallSnapshot";
#else
    static constexpr std::string_view ServiceName = "R";
    static constexpr std::string_view RequestVote = "0";
    static constexpr std::string_view AppendEntries = "1";
    static constexpr std::string_view InstallSnapshot = "2";
#endif
};

class KVService {
public:
#ifdef ENABLE_HUMAN_READABLE_RPC_NAMES
    static constexpr std::string_view ServiceName = "KVStorage";
    static constexpr std::string_view Put = "Put";
    static constexpr std::string_view Get = "Get";
    static constexpr std::string_view Delete = "Delete";
#else
    static constexpr std::string_view ServiceName = "K";
    static constexpr std::string_view Put = "0";
    static constexpr std::string_view Get = "1";
    static constexpr std::string_view Delete = "2";
#endif
};
} // namespace foskv::rpc