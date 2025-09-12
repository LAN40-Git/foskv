#include <kosio/fs.hpp>
#include <kosio/net.hpp>
#include <kosio/sync.hpp>
#include <kosio/core.hpp>
#include <kosio/signal.hpp>
#include <span>
#include <format>
#include <unordered_map>

constexpr std::string_view GET_OPTION = "GET ";
constexpr std::string_view PUT_OPTION = "PUT ";

class KVStore {
public:
    explicit KVStore(kosio::fs::File&& wal_file)
        : wal_file_(std::move(wal_file)) {}

    KVStore(KVStore&& other) noexcept
        : wal_file_(std::move(other.wal_file_)) {}
    auto operator=(KVStore&& other) noexcept -> KVStore& {
        wal_file_ = std::move(other.wal_file_);
        return *this;
    }

public:
    auto put(std::string key, std::string value) -> kosio::async::Task<void> {
        co_await mutex_.lock();
        std::lock_guard lock{mutex_, std::adopt_lock};
        kosio::log::console.info("PUT {} {}", key, value);
        co_await wal_file_.write_all(
            std::format("PUT {} {}\n", key, value)
        );
        data_[std::move(key)] = std::move(value);
    }

    auto get(std::string_view key) -> kosio::async::Task<std::string> {
        co_await mutex_.lock();
        std::lock_guard lock{mutex_, std::adopt_lock};
        kosio::log::console.info("{}", key);
        if (auto it = data_.find(std::string{key}); it != data_.end()) {
            co_return it->second;
        }
        co_return "nil";
    }

public:
    static auto open(std::string_view path) -> kosio::async::Task<kosio::Result<KVStore, kosio::IoError>> {
        if (auto has_file = co_await
        kosio::fs::File::options()
        .write(true)
        .create(true)
        .permission(0600)
        .open(path); has_file) {
            co_return KVStore{std::move(has_file.value())};
        } else {
            co_return std::unexpected{has_file.error()};
        }
    }

private:
    kosio::sync::Mutex                           mutex_{};
    kosio::fs::File                              wal_file_;
    std::unordered_map<std::string, std::string> data_{};
};

auto process(kosio::net::TcpStream stream, KVStore& store) -> kosio::async::Task<void> {
    char buf[1024];
    while (true) {
        auto ret = co_await stream.read(buf);
        if (!ret) {
            kosio::log::console.error("Failed to read from stream");
            break;
        }
        if (ret.value() == 0) {
            kosio::log::console.info("Client disconnected");
            break;
        }

        std::string_view sv{buf};

        // 去除换行符
        if (auto pos = sv.find('\n'); pos != std::string_view::npos) {
            sv = sv.substr(0, pos);
        }

        if (sv.starts_with(GET_OPTION)) {
            auto key = sv.substr(GET_OPTION.size());
            auto value = co_await store.get(key);
            co_await stream.write_all(value+'\n');
        } else if (sv.starts_with(PUT_OPTION)) {
            auto space_ops = sv.find(' ', PUT_OPTION.size());
            if (space_ops == std::string_view::npos) {
                co_await stream.write_all("nil\n");
            }
            auto key = sv.substr(PUT_OPTION.size(), space_ops - PUT_OPTION.size());
            auto value = sv.substr(space_ops+1);
            co_await store.put(std::string{key}, std::string{value});
            co_await stream.write_all("OK\n");
        }
    }
}

auto kv_server(KVStore& store) -> kosio::async::Task<void> {
    auto has_addr = kosio::net::SocketAddr::parse("localhost", 8080);
    if (!has_addr) {
        kosio::log::console.error("{}", has_addr.error());
        co_return;
    }
    auto has_listener = kosio::net::TcpListener::bind(has_addr.value());
    if (!has_listener) {
        kosio::log::console.error("{}", has_listener.error());
        co_return;
    }
    auto listener = std::move(has_listener.value());
    while (true) {
        auto ret = co_await listener.accept();
        if (!ret) {
            kosio::log::console.error("{}", ret.error());
            break;
        }
        auto& [stream, peer_addr] = ret.value();
        kosio::log::console.info("Accept connection from {}", peer_addr);
        kosio::spawn(process(std::move(stream), store));
    }
}

auto main_loop() -> kosio::async::Task<void> {
    auto ret = co_await KVStore::open("./kvstore.wal");
    if (!ret) {
        kosio::log::console.error("{}", ret.error());
        co_return;
    }
    auto store = std::move(ret.value());
    kosio::spawn(kv_server(store));
    co_await kosio::signal::ctrl_c();
}

auto main() -> int {
    kosio::runtime::MultiThreadBuilder::default_create().block_on(main_loop());
}