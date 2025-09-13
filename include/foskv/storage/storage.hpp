#pragma once
#include "foskv/common/util/noncopyable.hpp"
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
    static auto Open(const std::string& db_path) -> std::optional<KVStorage>;

public:
    auto Put(std::string_view key, std::string_view value) const -> rocksdb::Status;
    auto Get(std::string_view key, std::string* value) const -> rocksdb::Status;
    auto Delete(std::string_view key) const -> rocksdb::Status;
    auto BatchWrite(const std::vector<std::pair<std::string, std::string>>& kvs) const -> rocksdb::Status;
    auto MultiGet(const std::vector<std::string_view>& keys, std::vector<std::string>* values) const -> std::vector<rocksdb::Status>;

private:
   std::unique_ptr<rocksdb::DB> db_;
};
} // namespace foskv::storage