#pragma once
#include "foskv/raft/peer.hpp"
#include <kosio/fs.hpp>
#include <nlohmann/json.hpp>

namespace foskv::raft {
namespace detail {
    constexpr std::string_view PERSISTENT_KEY = "P";
    constexpr std::string_view START_LOG_INDEX = "S";
    constexpr std::string_view END_LOG_INDEX = "E";
    constexpr std::string_view PERSISTENT_PATH{"member/persistent"};
    constexpr std::string_view USER_DATA_PATH{"usr/data"};
} // namespace detail

struct NodeInfo {
    std::string name;
    std::string host;
    uint16_t    port;

    auto operator==(const NodeInfo& other) const noexcept -> bool;
    [[nodiscard]]
    auto hash() const noexcept -> uint64_t;
};

class RaftConfig {
    using PeerMap = std::unordered_map<uint64_t, detail::Peer>;
private:
    explicit RaftConfig(
        uint64_t cluster_id,
        uint64_t member_id,
        std::string&& name,
        const kosio::net::SocketAddr& addr,
        PeerMap&& peers,
        std::string_view path,
        nlohmann::json&& json);

public:
    RaftConfig(RaftConfig&& other) noexcept;
    auto operator=(RaftConfig&& other) noexcept -> RaftConfig&;

public:
    [[REMEMBER_CO_AWAIT]]
    static auto save(
        std::string_view path,
        uint64_t cluster_id,
        std::string_view name,
        const std::unordered_set<NodeInfo>& node_infos) -> kosio::async::Task<Result<void>>;

    [[REMEMBER_CO_AWAIT]]
    static auto load(std::string_view path) -> kosio::async::Task<Result<RaftConfig>>;

public:
    auto add_peer(NodeInfo peer_node_info) -> kosio::async::Task<Result<void>>;
    auto remove_peer(NodeInfo peer_node_info) -> kosio::async::Task<Result<void>>;
    auto remove_peer(uint64_t member_id) -> kosio::async::Task<Result<void>>;

private:
    auto save() -> kosio::async::Task<Result<void>>;

public:
    uint64_t               cluster_id_;
    uint64_t               member_id_;
    std::string            name_;
    kosio::net::SocketAddr addr_;
    PeerMap                peers_;
    std::filesystem::path  path_;
    nlohmann::json         json_;
};
} // namespace foskv::raft

template <>
struct std::hash<foskv::raft::NodeInfo> {
    std::size_t operator()(const foskv::raft::NodeInfo& n) const noexcept {
        return n.hash();
    }
};
