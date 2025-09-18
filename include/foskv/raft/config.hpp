#pragma once
#include <nlohmann/json.hpp>
#include "foskv/common/error.hpp"
#include "foskv/raft/peer.hpp"

namespace foskv::raft {
struct NodeInfo {
    std::string name;
    std::string host;
    uint16_t    port;
};

class Config {
public:
    explicit Config(
        uint64_t cluster_id,
        uint64_t member_id,
        const kosio::net::SocketAddr& node_addr,
        std::unordered_map<uint64_t, detail::Peer> peers)
        : cluster_id_(cluster_id)
        , member_id_(member_id)
        , node_addr_(node_addr)
        , peers_(std::move(peers)) {}

    /// @brief Save the raft config to a file
    /// @param path The config file path
    /// @param cluster_id The raft cluster id
    /// @param node_name The local raft node name
    /// @param node_infos The raft node information in this cluster
    /// @return void or RaftError
    static auto save(
        std::string_view path,
        uint64_t cluster_id,
        std::string_view node_name,
        const std::unordered_set<NodeInfo>& node_infos) -> RaftResult<void>;

    /// @brief Load the config from file
    /// @param path The config file path
    /// @return Config or RaftError
    static auto load(std::string_view path) -> RaftResult<Config>;

    uint64_t                                   cluster_id_;
    uint64_t                                   member_id_;
    kosio::net::SocketAddr                     node_addr_;
    std::unordered_map<uint64_t, detail::Peer> peers_;
};
} // namespace foskv::raft