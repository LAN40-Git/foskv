#pragma once
#include "foskv/common/error.hpp"
#include "foskv/raft/peer.hpp"
#include <kosio/fs.hpp>
#include <kosio/io/buf/stream.hpp>
#include <nlohmann/json.hpp>

namespace foskv::raft {
struct NodeInfo {
    std::string name;
    std::string host;
    uint16_t    port;

    auto operator==(const NodeInfo& other) const noexcept -> bool;
    auto hash() const noexcept -> uint64_t;
};

class Config {
public:
    explicit Config(
        uint64_t cluster_id,
        uint64_t local_member_id,
        std::string&& local_name,
        const kosio::net::SocketAddr& local_addr,
        std::unordered_map<uint64_t, detail::Peer>&& peers,
        kosio::io::BufStream<kosio::fs::File>&& writer);

public:
    [[REMEMBER_CO_AWAIT]]
    static auto create(
        std::string_view path,
        uint64_t cluster_id,
        std::string_view local_name,
        const std::unordered_set<NodeInfo>& node_infos) -> kosio::async::Task<RaftResult<void>>;

    [[REMEMBER_CO_AWAIT]]
    static auto load(std::string_view path) -> kosio::async::Task<RaftResult<Config>>;

public:
    auto save() -> RaftResult<void>;

    uint64_t                                   cluster_id_;
    uint64_t                                   local_member_id_;
    std::string                                local_name_;
    kosio::net::SocketAddr                     local_addr_;
    std::unordered_map<uint64_t, detail::Peer> peers_;
    kosio::io::BufStream<kosio::fs::File>      writer_;
};
} // namespace foskv::raft

namespace std {
    template <>
    struct hash<foskv::raft::NodeInfo> {
        std::size_t operator()(const foskv::raft::NodeInfo& n) const noexcept {
            return n.hash();
        }
    };
} // namespace std
