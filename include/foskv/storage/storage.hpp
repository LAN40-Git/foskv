#pragma once
#include "foskv/common/error.hpp"
#include "foskv/common/util/noncopyable.hpp"
#include <rocksdb/db.h>
#include <filesystem>

namespace foskv::storage {
class Storage : util::Noncopyable {
private:
    explicit Storage(rocksdb::DB* db);

public:
    ~Storage();
    Storage(Storage&& other) noexcept;
    auto operator=(Storage&& other) noexcept -> Storage&;

public:
    [[nodiscard]]
    static auto Open(const rocksdb::Options &options, const std::filesystem::path& db_path)
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
} // namespace foskv::storage