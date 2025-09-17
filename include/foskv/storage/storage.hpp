#pragma once
#include "foskv/common/util/noncopyable.hpp"
#include "foskv/common/error.hpp"
#include "foskv/storage/kvstorage.pb.h"
#include "foskv/storage/rpc.hpp"
#include <rocksdb/db.h>

namespace foskv::storage {
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
    auto RpcPut(std::string_view payload, std::span<char> response) const -> RpcResult<std::size_t>;
    [[nodiscard]]
    auto RpcGet(std::string_view payload, std::span<char> response) const -> RpcResult<std::size_t>;
    [[nodiscard]]
    auto RpcDelete(std::string_view payload, std::span<char> response) const -> RpcResult<std::size_t>;
    [[nodiscard]]
    auto BatchWrite(const std::vector<std::pair<std::string, std::string>>& kvs) const -> rocksdb::Status;
    [[nodiscard]]
    auto MultiGet(const std::vector<std::string_view>& keys, std::vector<std::string>* values) const -> std::vector<rocksdb::Status>;

private:
   std::unique_ptr<rocksdb::DB> db_;
};
} // namespace foskv::storage