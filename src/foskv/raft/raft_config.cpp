#include "foskv/raft/raft_config.hpp"
#include <kosio/common/debug.hpp>
#include <xxh3.h>

auto foskv::raft::NodeInfo::operator==(const NodeInfo &other) const noexcept -> bool {
    return name == other.name &&
           host == other.host &&
           port == other.port;
}

auto foskv::raft::NodeInfo::hash() const noexcept -> uint64_t {
    XXH3_state_t state;
    XXH3_64bits_reset(&state);

    XXH3_64bits_update(&state, name.data(), name.size());
    XXH3_64bits_update(&state, host.data(), host.size());
    XXH3_64bits_update(&state, &port, sizeof(port));

    return XXH3_64bits_digest(&state);
}

foskv::raft::RaftConfig::RaftConfig(
    const uint64_t cluster_id,
    const uint64_t member_id,
    std::string&& name,
    const kosio::net::SocketAddr &addr,
    std::unordered_map<uint64_t, detail::Peer>&& peers,
    std::string_view path,
    nlohmann::json&& json)
    : cluster_id_(cluster_id)
    , member_id_(member_id)
    , name_(name)
    , addr_(addr)
    , peers_(std::move(peers))
    , path_(path)
    , json_(std::move(json)) {}

foskv::raft::RaftConfig::RaftConfig(RaftConfig &&other) noexcept
    : cluster_id_(other.cluster_id_)
    , member_id_(other.member_id_)
    , name_(std::move(other.name_))
    , addr_(other.addr_)
    , peers_(std::move(other.peers_))
    , path_(std::move(other.path_))
    , json_(std::move(other.json_)) {}

auto foskv::raft::RaftConfig::operator=(RaftConfig &&other) noexcept -> RaftConfig & {
    cluster_id_ = other.cluster_id_;
    member_id_ = other.member_id_;
    name_ = std::move(other.name_);
    addr_ = other.addr_;
    peers_ = std::move(other.peers_);
    path_ = std::move(other.path_);
    json_ = std::move(other.json_);
    return *this;
}

auto foskv::raft::RaftConfig::save(
    std::string_view path,
    uint64_t cluster_id,
    std::string_view name,
    const std::unordered_set<NodeInfo>& node_infos) -> kosio::async::Task<Result<void>> {
    std::filesystem::path config_file_path(path);
    if (config_file_path.has_parent_path()) {
        std::filesystem::create_directories(config_file_path.parent_path());
    }
    auto has_config_file = co_await kosio::fs::File::options()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .permission(0600)
        .open(path);
    if (!has_config_file) {
        LOG_ERROR("{}", has_config_file.error());
        co_return std::unexpected{make_error(Error::kRaftConfigFileOpenFailed)};
    }
    auto config_file = std::move(has_config_file.value());

    nlohmann::json json;
    std::optional<uint64_t> local_member_id{std::nullopt};
    std::optional<std::string> local_host{std::nullopt};
    std::optional<uint16_t> local_port{std::nullopt};

    nlohmann::json nodes_json = nlohmann::json::array();
    for (const auto& node_info : node_infos) {
        // Check the peer address
        auto has_addr = kosio::net::SocketAddr::parse(node_info.host, node_info.port);
        if (!has_addr) {
            co_return std::unexpected{make_error(Error::kInvalidPeerAddress)};
        }

        // Generate member_id
        auto member_id = node_info.hash();
        if (node_info.name == name) {
            local_member_id = member_id;
            local_host = node_info.host;
            local_port = node_info.port;
        }

        nlohmann::json node_entry;
        node_entry["member_id"] = member_id;
        node_entry["name"] = node_info.name;
        node_entry["host"] = node_info.host;
        node_entry["port"] = node_info.port;
        nodes_json.push_back(node_entry);
    }

    if (!local_member_id.has_value() || !local_host.has_value() || !local_port.has_value()) {
        co_return std::unexpected{make_error(Error::kLocalNodeNotFound)};
    }

    json["cluster_id"] = cluster_id;
    json["member_id"] = local_member_id.value();
    json["name"] = name;
    json["host"] = local_host.value();
    json["port"] = local_port.value();
    json["nodes"] = nodes_json;

#ifdef ENABLE_HUMAN_READABLE_JSON
    auto config_payload = json.dump(4);
#else
    auto config_payload = nodes_json.dump(-1);
#endif
    if (auto ret = co_await config_file.write_all(config_payload); !ret) {
        co_return std::unexpected{make_error(Error::kRaftConfigFileWriteFailed)};
    }
    co_return Result<void>{};
}

auto foskv::raft::RaftConfig::load(std::string_view path)
-> kosio::async::Task<Result<RaftConfig>> {
    std::filesystem::path config_file_path(path);
    std::ifstream config_file(config_file_path);

    if (!config_file) {
        co_return std::unexpected{make_error(errno)};
    }

    try {
        auto json = nlohmann::json::parse(config_file);
        config_file.close();

        auto cluster_id = json["cluster_id"].get<uint64_t>();
        auto local_member_id = json["member_id"].get<std::uint64_t>();
        auto local_name = json["name"].get<std::string>();
        auto local_host = json["host"].get<std::string>();
        auto local_port = json["port"].get<uint16_t>();
        auto has_node_addr = kosio::net::SocketAddr::parse(local_host, local_port);
        if (!has_node_addr) {
            LOG_ERROR("{}", has_node_addr.error());
            co_return std::unexpected{make_error(Error::kInvalidLocalAddress)};
        }

        PeerMap peers;
        const auto& nodes_json = json["nodes"];
        for (const auto& node_json : nodes_json) {
            NodeInfo node_info;
            auto member_id = node_json["member_id"].get<uint64_t>();
            node_info.name = node_json["name"].get<std::string>();
            node_info.host = node_json["host"].get<std::string>();
            node_info.port = node_json["port"].get<uint16_t>();

            // Check repeat
            if (peers.contains(member_id)) {
                co_return std::unexpected{make_error(Error::kRepeatedPeer)};
            }

            auto has_peer = co_await detail::Peer::create(member_id, node_info.name, node_info.host, node_info.port);
            if (!has_peer) {
                co_return std::unexpected{has_peer.error()};
            }
            peers.emplace(member_id, std::move(has_peer.value()));
        }

        co_return RaftConfig{
            cluster_id,
            local_member_id,
            std::move(local_name),
            has_node_addr.value(),
            std::move(peers),
            path,
            std::move(json)};
    } catch (const nlohmann::json::parse_error& e) {
        LOG_VERBOSE("Failed to parse json file", e.what());
        throw;
    } catch (...) {
        LOG_VERBOSE("Unknown exception while loading config");
        throw;
    }
}

auto foskv::raft::RaftConfig::add_peer(NodeInfo peer_node_info) -> kosio::async::Task<Result<void>> {
    auto member_id = peer_node_info.hash();
    auto has_peer = co_await detail::Peer::create(member_id, peer_node_info.name, peer_node_info.host, peer_node_info.port);
    if (!has_peer) {
        co_return std::unexpected{has_peer.error()};
    }
    peers_.emplace(member_id, std::move(has_peer.value()));

    // TODO: Release the lock
    auto& nodes_json = json_["nodes"];
    nlohmann::json node_entry;
    node_entry["member_id"] = member_id;
    node_entry["name"] = peer_node_info.name;
    node_entry["host"] = peer_node_info.host;
    node_entry["port"] = peer_node_info.port;
    nodes_json.push_back(node_entry);

    co_return co_await this->save();
}

auto foskv::raft::RaftConfig::remove_peer(NodeInfo peer_node_info) -> kosio::async::Task<Result<void>> {
    auto member_id = peer_node_info.hash();
    auto peer = peers_.find(member_id);
    if (peer == peers_.end()) {
        LOG_ERROR("Failed to find peer {}", member_id);
        co_return std::unexpected{make_error(Error::kPeerNotFound)};
    }
    co_await peer->second.shutdown();
    peers_.erase(member_id);
    auto& nodes_json = json_["nodes"];
    nodes_json.erase(std::ranges::find_if(nodes_json,
    [member_id](const auto& node) {
      return node["member_id"] == member_id;
    }));

    co_return co_await save();
}

auto foskv::raft::RaftConfig::remove_peer(uint64_t member_id)
-> kosio::async::Task<Result<void>> {
    auto peer = peers_.find(member_id);
    if (peer == peers_.end()) {
        LOG_ERROR("Failed to find peer {}", member_id);
        co_return std::unexpected{make_error(Error::kPeerNotFound)};
    }
    co_await peer->second.shutdown();
    peers_.erase(member_id);
    auto& nodes_json = json_["nodes"];
    nodes_json.erase(std::ranges::find_if(nodes_json,
    [member_id](const auto& node) {
      return node["member_id"] == member_id;
    }));

    co_return co_await save();
}

auto foskv::raft::RaftConfig::save() -> kosio::async::Task<Result<void>> {
#ifdef ENABLE_HUMAN_READABLE_JSON
    auto config_payload = json_.dump(4);
#else
    auto config_payload = nodes_json.dump(-1);
#endif

    constexpr std::string_view TEMP_FILE_NAME = "tmp";
    auto temp_file_path = (path_.parent_path()/TEMP_FILE_NAME).string();
    // Open temp file
    auto has_temp_file = co_await kosio::fs::File::options()
        .create(true)
        .write(true)
        .truncate(true)
        .permission(0600)
        .open(temp_file_path);
    if (!has_temp_file) {
        LOG_ERROR("{}", has_temp_file.error());
        co_return std::unexpected{make_error(Error::kTempRaftConfigFileOpenFailed)};
    }

    auto temp_file = std::move(has_temp_file.value());
    if (auto ret = co_await temp_file.write_all(config_payload); !ret) {
        LOG_ERROR("{}", ret.error());
        co_return std::unexpected{make_error(Error::kTempRaftConfigFileWriteFailed)};
    }

    if (auto ret = co_await kosio::io::rename(temp_file_path.c_str(), path_.c_str()); !ret) {
        LOG_ERROR("{}", ret.error());
        co_return std::unexpected{make_error(Error::kTempRaftConfigFileRenameFailed)};
    }
    // Successful save
    LOG_VERBOSE("Successfully saved config : {}", config_payload);
    co_return Result<void>{};
}
