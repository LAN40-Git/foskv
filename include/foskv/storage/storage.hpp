#pragma once
#include "foskv/common/util/noncopyable.hpp"
#include "foskv/common/error.hpp"
#include "foskv/storage/kvstorage.pb.h"
#include "foskv/storage/rpc.hpp"
#include <rocksdb/db.h>

namespace foskv::storage {
class Storage {
public:
    explicit Storage(rocksdb::DB* db);
    ~Storage();

    Storage(const Storage& other);
    auto operator=(const Storage& other) -> Storage&;
    Storage(Storage&& other) noexcept;
    auto operator=(Storage&& other) noexcept -> Storage&;

public:
    [[nodiscard]]
    static auto Open(const rocksdb::Options &options, std::string_view db_path)
    -> StorageResult<Storage>;

public:
    void SetWriteOptions(const rocksdb::WriteOptions &write_options);
    void SetReadOptions(const rocksdb::ReadOptions &read_options);

public:
    [[nodiscard]]
    auto Put(const rocksdb::Slice& key, const rocksdb::Slice& value) const -> rocksdb::Status;
    [[nodiscard]]
    auto Get(const rocksdb::Slice& key, std::string* value) const -> rocksdb::Status;
    [[nodiscard]]
    auto Delete(const rocksdb::Slice& key) const -> rocksdb::Status;
    [[nodiscard]]
    auto BatchWrite(rocksdb::WriteBatch& write_batch) const -> rocksdb::Status;
    [[nodiscard]]
    auto MultiGet(const std::vector<rocksdb::Slice>& keys, std::vector<std::string> &values)
    const -> std::vector<rocksdb::Status>;

private:
    rocksdb::DB          *db_;
    rocksdb::WriteOptions write_options_;
    rocksdb::ReadOptions  read_options_;
};

class KVStorage : public util::Noncopyable {
public:
    explicit KVStorage(rocksdb::DB* db)
        : db_(db) {}

    KVStorage(KVStorage&& other) noexcept : db_(std::move(other.db_)) {}
    auto operator=(KVStorage&& other) noexcept -> KVStorage& {
        db_ = std::move(other.db_);
        return *this;
    }

public:
    [[nodiscard]]
    static auto Open(const std::string& db_path) -> StorageResult<KVStorage>;

public:
    [[nodiscard]]
    auto Put(std::string_view key, std::string_view value) const -> rocksdb::Status;
    [[nodiscard]]
    auto Get(std::string_view key, std::string* value) const -> rocksdb::Status;
    [[nodiscard]]
    auto Delete(std::string_view key) const -> rocksdb::Status;
    [[nodiscard]]
    auto RpcPut(std::string_view req_payload, std::span<char> resp_payload) const -> RpcResult<std::size_t>;
    [[nodiscard]]
    auto RpcGet(std::string_view req_payload, std::span<char> resp_payload) const -> RpcResult<std::size_t>;
    [[nodiscard]]
    auto RpcDelete(std::string_view req_payload, std::span<char> resp_payload) const -> RpcResult<std::size_t>;
    [[nodiscard]]
    auto BatchWrite(const std::vector<std::pair<std::string, std::string>>& kvs) const -> rocksdb::Status;
    [[nodiscard]]
    auto MultiGet(const std::vector<std::string_view>& keys, std::vector<std::string>* values) const -> std::vector<rocksdb::Status>;

private:
   std::unique_ptr<rocksdb::DB> db_;
};
} // namespace foskv::storage