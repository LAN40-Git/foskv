#pragma once
#include <string_view>

namespace foskv::raft {
class RaftRpc {
public:
#ifdef USE_RPC_NAME
    static constexpr std::string_view RequestVote = "RequestVote";
    static constexpr std::string_view AppendEntries = "AppendEntries";
    static constexpr std::string_view InstallSnapshot = "InstallSnapshot";
#else
    static constexpr std::string_view RequestVote = "R0";
    static constexpr std::string_view AppendEntries = "R1";
    static constexpr std::string_view InstallSnapshot = "R2";
#endif
};
} // namespace foskv::raft