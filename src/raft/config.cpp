#include "foskv/raft/config.hpp"
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

foskv::raft::Config::Config(
    uint64_t cluster_id,
    uint64_t local_member_id,
    std::string&& local_name,
    const kosio::net::SocketAddr &local_addr,
    std::unordered_map<uint64_t, detail::Peer>&& peers,
    std::fstream&& config_file)
    : cluster_id_(cluster_id)
    , local_member_id_(local_member_id)
    , local_name_(local_name)
    , local_addr_(local_addr)
    , peers_(std::move(peers))
    , config_file_(std::move(config_file)) {}

auto foskv::raft::Config::create(
    std::string_view path,
    uint64_t cluster_id,
    std::string_view local_name,
    const std::unordered_set<NodeInfo>& node_infos) -> kosio::async::Task<RaftResult<void>> {
    std::filesystem::path config_file_path(path);
    std::filesystem::create_directories(config_file_path.parent_path());

    auto has_config_file = co_await kosio::fs::File::options()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path);
    if (!has_config_file) {
        co_return std::unexpected{make_raft_error(RaftError::kConfigFileOpenFailed)};
    }

    auto config_file = std::move(has_config_file.value());

    nlohmann::json config_json;
    std::optional<uint64_t> local_member_id{std::nullopt};
    std::optional<std::string> local_host{std::nullopt};
    std::optional<uint16_t> local_port{std::nullopt};

    nlohmann::json nodes_json = nlohmann::json::array();
    for (const auto& node_info : node_infos) {
        // Check the peer address
        auto has_addr = kosio::net::SocketAddr::parse(node_info.host, node_info.port);
        if (!has_addr) {
            co_return std::unexpected{make_raft_error(RaftError::kInvalidPeerAddress)};
        }

        // Generate member_id
        auto member_id = node_info.hash();
        if (node_info.name == local_name) {
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
        co_return std::unexpected{make_raft_error(RaftError::kLocalNodeNotFound)};
    }

    config_json["cluster_id"] = cluster_id;
    config_json["member_id"] = local_member_id.value();
    config_json["name"] = local_name;
    config_json["host"] = local_host.value();
    config_json["port"] = local_port.value();
    config_json["nodes"] = nodes_json;

    auto config_json_payload = config_json.dump();
    if (auto ret = co_await config_file.write_all(config_json_payload); !ret) {
        co_return std::unexpected{make_raft_error(RaftError::kConfigFileWriteFailed)};
    }
    co_return RaftResult<void>{};
}

auto foskv::raft::Config::load(std::string_view path) -> kosio::async::Task<RaftResult<Config>> {
    auto has_config_file = co_await kosio::fs::File::options()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(path);
    if (!has_config_file) {
        co_return std::unexpected{make_raft_error(RaftError::kConfigFileOpenFailed)};
    }
    auto config_file = std::move(has_config_file.value());

    try {
        auto config_json = nlohmann::json::parse(config_stream);
        config_stream.close();
        auto cluster_id = config_json["cluster_id"].get<uint64_t>();
        auto local_member_id = config_json["member_id"].get<std::uint64_t>();
        auto local_name = config_json["name"].get<std::string>();
        auto local_host = config_json["host"].get<std::string>();
        auto local_port = config_json["port"].get<uint16_t>();
        auto has_node_addr = kosio::net::SocketAddr::parse(local_host, local_port);
        if (!has_node_addr) {
            co_return std::unexpected{make_raft_error(RaftError::kInvalidNodeAddress)};
        }

        std::unordered_map<uint64_t, detail::Peer> peers;
        const auto& nodes_json = config_json["nodes"];
        for (const auto& node_json : nodes_json) {
            NodeInfo node_info;
            auto member_id = node_json["member_id"].get<uint64_t>();
            node_info.name = node_json["name"].get<std::string>();
            node_info.host = node_json["host"].get<std::string>();
            node_info.port = node_json["port"].get<uint16_t>();

            // Check repeat
            if (peers.contains(member_id)) {
                co_return std::unexpected{make_raft_error(RaftError::kRepeatedPeer)};
            }

            auto has_addr = kosio::net::SocketAddr::parse(node_info.host, node_info.port);
            if (!has_addr) {
                co_return std::unexpected{make_raft_error(RaftError::kInvalidPeerAddress)};
            }
            peers.emplace(member_id, detail::Peer{member_id, node_info.name, has_addr.value()});
        }

        return Config{cluster_id, local_member_id, std::move(local_name), has_node_addr.value(), std::move(peers), std::move(config_file)};
    } catch (const nlohmann::json::parse_error& e) {
        LOG_ERROR("Failed to parse json file : {}", e.what());
        return std::unexpected{make_raft_error(RaftError::kJsonParseFailed)};
    } catch (...) {
        LOG_ERROR("Unknown exception while loading config");
        return std::unexpected{make_raft_error(RaftError::kUnknown)};
    }
}
