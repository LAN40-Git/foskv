#include "foskv/raft/config.hpp"

auto foskv::raft::Config::save(std::string_view path, uint64_t cluster_id,
    uint64_t member_id, std::string_view host, uint16_t port) -> RaftResult<Config> {
    auto has_addr = kosio::net::SocketAddr::parse(host, port);
    if (!has_addr) [[unlikely]] {
        return std::unexpected{make_raft_error(RaftError::kConfigSaveFailed)};
    }

    std::filesystem::path config_file_path(path);
    std::filesystem::create_directories(config_file_path.parent_path());
    std::ofstream config_file(config_file_path);

    if (!config_file) {
        return std::unexpected{make_raft_error(RaftError::kConfigSaveFailed)};
    }

    nlohmann::json config_json;
    config_json["cluster_id"] = cluster_id;
    config_json["member_id"] = member_id;
    config_json["host"] = host;
    config_json["port"] = port;
    config_file << config_json.dump(4);
    return Config(cluster_id, member_id, has_addr.value());
}

auto foskv::raft::Config::load(std::string_view path) -> RaftResult<Config> {
    std::filesystem::path config_file_path(path);
    std::ifstream config_file(config_file_path);

    if (!config_file) {
        LOG_ERROR("{}", strerror(errno));
        return std::unexpected{make_raft_error(RaftError::kConfigLoadFailed)};
    }

    try {
        auto config_json = nlohmann::json::parse(config_file);

        for (const auto& key : {"cluster_id", "member_id", "host", "port"}) {
            if (!config_json.contains(key)) {
                return std::unexpected{make_raft_error(RaftError::kConfigLoadFailed)};
            }
        }

        auto cluster_id = config_json["cluster_id"].get<uint64_t>();
        auto member_id = config_json["member_id"].get<uint64_t>();
        auto host = config_json["host"].get<std::string>();
        auto port = config_json["port"].get<uint16_t>();

        auto has_addr = kosio::net::SocketAddr::parse(host, port);
        if (!has_addr) [[unlikely]] {
            return std::unexpected{make_raft_error(RaftError::kConfigLoadFailed)};
        }
        return Config{cluster_id, member_id, has_addr.value()};
    } catch (const nlohmann::json::parse_error& e) {
        LOG_ERROR("{}", e.what());
        return std::unexpected{make_raft_error(RaftError::kConfigLoadFailed)};
    } catch (...) {
        return std::unexpected{make_raft_error(RaftError::kUnknown)};
    }
}
