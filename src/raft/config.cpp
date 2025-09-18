#include "foskv/raft/config.hpp"
#include <kosio/common/debug.hpp>

auto foskv::raft::Config::save(
    std::string_view path,
    uint64_t cluster_id,
    std::string_view node_name,
    const std::unordered_set<NodeInfo>& node_infos) -> RaftResult<void> {
    std::filesystem::path config_file_path(path);
    std::filesystem::create_directories(config_file_path.parent_path());
    std::ofstream config_file(config_file_path);

    if (!config_file) {
        LOG_ERROR("Failed to open config file {} : {}", path, strerror(errno));
        return std::unexpected{make_raft_error(RaftError::kConfigFileOpenFailed)};
    }

    nlohmann::json config_json;
    config_json["cluster_id"] = cluster_id;
    config_json["node_name"] = node_name;

    nlohmann::json nodes_json = nlohmann::json::array();
    for (const auto& node_info : node_infos) {
        // Check the peer address
        auto has_addr = kosio::net::SocketAddr::parse(node_info.host, node_info.port);
        if (!has_addr) {
            return std::unexpected{make_raft_error(RaftError::kInvalidPeerAddress)};
        }

        nlohmann::json node_entry;
        node_entry["name"] = node_info.name;
        node_entry["host"] = node_info.host;
        node_entry["port"] = node_info.port;
        nodes_json.push_back(node_entry);
    }
    config_json["nodes"] = nodes_json;
    config_file << config_json.dump(4);

    return RaftResult<void>{};
}

auto foskv::raft::Config::load(std::string_view path) -> RaftResult<Config> {
    std::filesystem::path config_file_path(path);
    std::ifstream config_file(config_file_path);

    if (!config_file) {
        LOG_ERROR("Failed to open config file {} : {}", path, strerror(errno));
        return std::unexpected{make_raft_error(RaftError::kConfigFileOpenFailed)};
    }

    try {
        auto config_json = nlohmann::json::parse(config_file);
        auto cluster_id = config_json["cluster_id"].get<uint64_t>();
        auto node_name = config_json["node_name"].get<std::string>();
        std::optional<uint64_t> node_id{std::nullopt};
        std::optional<kosio::net::SocketAddr> node_addr{std::nullopt};

        std::unordered_map<uint64_t, detail::Peer> peers;
        const auto& nodes_json = config_json["nodes"];
        for (const auto& node_json : nodes_json) {
            NodeInfo node_info;
            node_info.name = node_json["name"].get<std::string>();
            node_info.host = node_json["host"].get<std::string>();
            node_info.port = node_json["port"].get<uint16_t>();

            uint64_t member_id = std::hash<std::string>{}(node_info.name + node_info.host);
            // Check repeat
            if (peers.contains(member_id)) [[unlikely]] {
                return std::unexpected{make_raft_error(RaftError::kRepeatedPeer)};
            }

            auto has_addr = kosio::net::SocketAddr::parse(node_info.host, node_info.port);
            if (!has_addr) [[unlikely]] {
                return std::unexpected{make_raft_error(RaftError::kInvalidPeerAddress)};
            }

            if (node_info.name == node_name) {
                node_id = member_id;
                node_addr = has_addr.value();
            }
            peers.emplace(member_id, detail::Peer{member_id, node_info.name, has_addr.value()});
        }

        if (!node_id.has_value() ||
            !node_addr.has_value()) [[unlikely]] {
            return std::unexpected{make_raft_error(RaftError::kLocalNodeNotFound)};
        }

        return Config{cluster_id, node_id.value(), node_addr.value(), std::move(peers)};
    } catch (const nlohmann::json::parse_error& e) {
        LOG_ERROR("Failed to parse json file : {}", e.what());
        return std::unexpected{make_raft_error(RaftError::kJsonParseFailed)};
    } catch (...) {
        return std::unexpected{make_raft_error(RaftError::kUnknown)};
    }
}
