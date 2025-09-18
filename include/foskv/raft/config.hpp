#pragma once
#include <nlohmann/json.hpp>
#include <kosio/common/error.hpp>
#include <kosio/common/debug.hpp>
#include <kosio/net/addr.hpp>
#include <fstream>
#include <string>
#include "foskv/common/error.hpp"

namespace foskv::raft {
class Config {
public:
    explicit Config(uint64_t cluster_id, uint64_t member_id, const kosio::net::SocketAddr& addr)
        : cluster_id_(cluster_id)
        , member_id_(member_id)
        , addr_(addr) {}

public:
    /// @brief Save the raft config to a file
    /// @param path The config file path
    /// @param cluster_id The raft node cluster id
    /// @param member_id The raft node member id
    /// @param host The raft node host
    /// @param port The raft node port
    /// @return RaftError or Config
    static auto save(std::string_view path, uint64_t cluster_id,
        uint64_t member_id, std::string_view host, uint16_t port) -> RaftResult<Config>;

    /// @brief Load the config from file
    /// @param path The config file path
    /// @return RaftError or Config
    static auto load(std::string_view path) -> RaftResult<Config>;

private:
    uint64_t               cluster_id_;
    uint64_t               member_id_;
    kosio::net::SocketAddr addr_;
};
} // namespace foskv::raft