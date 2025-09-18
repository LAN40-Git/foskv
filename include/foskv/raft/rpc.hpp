#pragma once
#include <string_view>

namespace foskv::raft {
class RaftRpc {
public:
#ifdef ENABLE_HUMAN_READABLE_RPC_NAMES
    static constexpr std::string_view ServiceName = "Raft";
    static constexpr std::string_view RequestVote = "RequestVote";
    static constexpr std::string_view AppendEntries = "AppendEntries";
    static constexpr std::string_view InstallSnapshot = "InstallSnapshot";
#else
    static constexpr std::string_view ServiceName = "R";
    static constexpr std::string_view RequestVote = "R0";
    static constexpr std::string_view AppendEntries = "R1";
    static constexpr std::string_view InstallSnapshot = "R2";
#endif
};
} // namespace foskv::raft