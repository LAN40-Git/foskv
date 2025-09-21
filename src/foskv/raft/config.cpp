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
    const uint64_t cluster_id,
    const uint64_t member_id,
    std::string&& name,
    const kosio::net::SocketAddr &addr,
    std::unordered_map<uint64_t, PeerPtr>&& peers,
    kosio::fs::File&& tmp_file,
    std::string_view config_path,
    nlohmann::json&& config_json)
    : cluster_id_(cluster_id)
    , member_id_(member_id)
    , name_(name)
    , addr_(addr)
    , peers_(std::move(peers))
    , tmp_file_(std::move(tmp_file))
    , config_path_(config_path)
    , config_json_(std::move(config_json)) {}

foskv::raft::Config::Config(Config &&other) noexcept
    : cluster_id_(other.cluster_id_)
    , member_id_(other.member_id_)
    , name_(std::move(other.name_))
    , addr_(other.addr_)
    , peers_(std::move(other.peers_))
    , tmp_file_(std::move(other.tmp_file_))
    , config_path_(std::move(other.config_path_))
    , config_json_(std::move(other.config_json_)) {}

auto foskv::raft::Config::operator=(Config &&other) noexcept -> Config & {
    cluster_id_ = other.cluster_id_;
    member_id_ = other.member_id_;
    name_ = std::move(other.name_);
    addr_ = other.addr_;
    peers_ = std::move(other.peers_);
    tmp_file_ = std::move(other.tmp_file_);
    config_path_ = std::move(other.config_path_);
    config_json_ = std::move(other.config_json_);
    return *this;
}

auto foskv::raft::Config::save(
    const std::filesystem::path& path,
    uint64_t cluster_id,
    std::string_view name,
    const std::unordered_set<NodeInfo>& node_infos) -> kosio::async::Task<RaftResult<void>> {
    std::filesystem::create_directories(path.parent_path());

    auto has_config_file = co_await kosio::fs::File::options()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .permission(0600)
        .open(path.string());
    if (!has_config_file) {
        LOG_ERROR("Failed to open config file {} : {}", path.string(), has_config_file.error());
        co_return std::unexpected{make_raft_error(RaftError::kTempConfigFileOpenFailed)};
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
        co_return std::unexpected{make_raft_error(RaftError::kLocalNodeNotFound)};
    }

    config_json["cluster_id"] = cluster_id;
    config_json["member_id"] = local_member_id.value();
    config_json["name"] = name;
    config_json["host"] = local_host.value();
    config_json["port"] = local_port.value();
    config_json["nodes"] = nodes_json;

#ifdef ENABLE_HUMAN_READABLE_JSON
    auto config_payload = config_json.dump(4);
#else
    auto config_payload = nodes_json.dump(-1);
#endif
    if (auto ret = co_await config_file.write_all(config_payload); !ret) {
        co_return std::unexpected{make_raft_error(RaftError::kConfigFileWriteFailed)};
    }
    co_return RaftResult<void>{};
}

auto foskv::raft::Config::load(std::string_view path)
-> kosio::async::Task<RaftResult<Config>> {
    std::filesystem::path config_file_path(path);
    std::ifstream config_stream(config_file_path);

    if (!config_stream) {
        co_return std::unexpected{make_raft_error(errno)};
    }

    try {
        auto config_json = nlohmann::json::parse(config_stream);
        config_stream.close();
        auto tmp_file_path = config_file_path.parent_path() / "tmp.json";
        auto has_tmp_config_file = co_await kosio::fs::File::options()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .permission(0600)
            .open(tmp_file_path.string());
        if (!has_tmp_config_file) {
            co_return std::unexpected{make_raft_error(errno)};
        }
        auto tmp_config_file = std::move(has_tmp_config_file.value());

        auto cluster_id = config_json["cluster_id"].get<uint64_t>();
        auto local_member_id = config_json["member_id"].get<std::uint64_t>();
        auto local_name = config_json["name"].get<std::string>();
        auto local_host = config_json["host"].get<std::string>();
        auto local_port = config_json["port"].get<uint16_t>();
        auto has_node_addr = kosio::net::SocketAddr::parse(local_host, local_port);
        if (!has_node_addr) {
            co_return std::unexpected{make_raft_error(RaftError::kInvalidLocalAddress)};
        }

        std::unordered_map<uint64_t, PeerPtr> peers;
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
            peers.emplace(member_id, std::make_unique<detail::Peer>(member_id, node_info.name, has_addr.value()));
        }

        co_return Config{
            cluster_id,
            local_member_id,
            std::move(local_name),
            has_node_addr.value(),
            std::move(peers),
            std::move(tmp_config_file),
            path,
            std::move(config_json)
        };
    } catch (const nlohmann::json::parse_error& e) {
        LOG_VERBOSE("Failed to parse json file", e.what());
        throw;
    } catch (...) {
        LOG_VERBOSE("Unknown exception while loading config");
        throw;
    }
}

auto foskv::raft::Config::add_peer(uint64_t member_id, std::string_view name,
        std::string_view host, uint16_t port) -> kosio::async::Task<RaftResult<void>> {
    // Hold raft node's mutex
    auto has_addr = kosio::net::SocketAddr::parse(host, port);
    if (!has_addr) {
        co_return std::unexpected{make_raft_error(RaftError::kInvalidPeerAddress)};
    }
    peers_.emplace(member_id, std::make_unique<detail::Peer>(member_id, name, has_addr.value()));

    auto& nodes_json = config_json_["nodes"];
    nlohmann::json node_entry;
    node_entry["member_id"] = member_id;
    node_entry["name"] = name;
    node_entry["host"] = host;
    node_entry["port"] = port;
    nodes_json.push_back(node_entry);

    co_return co_await save();
}

auto foskv::raft::Config::remove_peer(uint64_t member_id)
-> kosio::async::Task<RaftResult<void>> {
    // Hold raft node's mutex
    peers_.erase(member_id);
    auto& nodes_json = config_json_["nodes"];
    nodes_json.erase(std::ranges::find_if(nodes_json,
    [member_id](const auto& node) {
      return node["member_id"] == member_id;
    }));

    co_return co_await save();
}

auto foskv::raft::Config::save() -> kosio::async::Task<RaftResult<void>> {
#ifdef ENABLE_HUMAN_READABLE_JSON
    auto config_payload = config_json_.dump(4);
#else
    auto config_payload = nodes_json.dump(-1);
#endif

    if (auto ret = co_await tmp_file_.write_all(config_payload); !ret) [[unlikely]] {
        // Try to reopen the file
        co_await tmp_file_.close();
        auto has_file = co_await kosio::fs::File::options()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .permission(0600)
            .open((config_path_ / "tmp.json").string());
        if (!has_file) [[unlikely]] {
            LOG_FATAL("Failed to write and reopen temp raft config file : {}", (config_path_ / "tmp.json").string());
        }
        co_return std::unexpected{make_raft_error(RaftError::kConfigFileWriteFailed)};
    }

    // Atomic replace
    if (auto ret = co_await kosio::fs::rename((config_path_ / "tmp.json").string(), config_path_.string()); !ret) [[unlikely]] {
        LOG_FATAL("Failed to rename raft config file : {}", (config_path_ / "tmp.json").string());
        co_return std::unexpected{make_raft_error(RaftError::kConfigFileRenameFailed)};
    }
    co_return RaftResult<void>{};
}
