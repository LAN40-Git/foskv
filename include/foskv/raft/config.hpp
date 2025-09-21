#pragma once
#include "foskv/raft/peer.hpp"
#include <kosio/fs.hpp>
#include <nlohmann/json.hpp>

namespace foskv::raft {
namespace detail {
    static constexpr std::string_view PERSISTENT_KEY = "P";
    static constexpr std::string_view START_LOG_INDEX = "S";
    static constexpr std::string_view END_LOG_INDEX = "E";
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

class Config {
    using PeerPtr = std::unique_ptr<detail::Peer>;
private:
    explicit Config(
        uint64_t cluster_id,
        uint64_t member_id,
        std::string&& name,
        const kosio::net::SocketAddr& addr,
        std::unordered_map<uint64_t, PeerPtr>&& peers,
        kosio::fs::File&& tmp_file,
        std::string_view config_path,
        nlohmann::json&& config_json);

public:
    Config(Config&& other) noexcept;
    auto operator=(Config&& other) noexcept -> Config&;

public:
    [[REMEMBER_CO_AWAIT]]
    static auto save(
        const std::filesystem::path& path,
        uint64_t cluster_id,
        std::string_view name,
        const std::unordered_set<NodeInfo>& node_infos) -> kosio::async::Task<RaftResult<void>>;

    [[REMEMBER_CO_AWAIT]]
    static auto load(std::string_view path) -> kosio::async::Task<RaftResult<Config>>;

public:
    auto add_peer(uint64_t member_id, std::string_view name,
        std::string_view host, uint16_t port) -> kosio::async::Task<RaftResult<void>>;
    auto remove_peer(uint64_t member_id) -> kosio::async::Task<RaftResult<void>>;

private:
    auto save() -> kosio::async::Task<RaftResult<void>>;

public:
    uint64_t                              cluster_id_;
    uint64_t                              member_id_;
    std::string                           name_;
    kosio::net::SocketAddr                addr_;
    std::unordered_map<uint64_t, PeerPtr> peers_;
    kosio::fs::File                       tmp_file_;
    std::filesystem::path                 config_path_;
    nlohmann::json                        config_json_;
};
} // namespace foskv::raft

template <>
struct std::hash<foskv::raft::NodeInfo> {
    std::size_t operator()(const foskv::raft::NodeInfo& n) const noexcept {
        return n.hash();
    }
};
