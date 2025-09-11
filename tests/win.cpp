#include <kosio/fs.hpp>
#include <kosio/net.hpp>
#include <kosio/sync.hpp>
#include <kosio/core.hpp>
#include <kosio/signal.hpp>
#include <span>
#include <format>
#include <unordered_map>

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
        co_await wal_file_.write_all(
            std::format("PUT {} {}\n", key, value)
        );
        data_[std::move(key)] = std::move(value);
    }

    auto get(std::string_view key) -> kosio::async::Task<std::string> {
        co_await mutex_.lock();
        std::lock_guard lock{mutex_, std::adopt_lock};
        if (auto it = data_.find(key.data()); it != data_.end()) {
            co_return it->second;
        }
        co_return "nil\n";
    }

public:
    static auto open(std::string_view path) -> kosio::async::Task<kosio::Result<KVStore, kosio::IoError>> {
        if (auto has_file = co_await kosio::fs::File::options()
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