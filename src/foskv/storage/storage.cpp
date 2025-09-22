#include "foskv/storage/storage.hpp"
#include <kosio/common/debug.hpp>

foskv::storage::Storage::Storage(rocksdb::DB *db)
    : db_(db) {}

foskv::storage::Storage::~Storage() {
    if (db_ != nullptr) {
        db_->Close();
    }
}

foskv::storage::Storage::Storage(Storage &&other) noexcept {
    db_ = other.db_;
    other.db_ = nullptr;
}

auto foskv::storage::Storage::operator=(Storage &&other) noexcept -> Storage & {
    db_ = other.db_;
    other.db_ = nullptr;
    return *this;
}

auto foskv::storage::Storage::Open(const rocksdb::Options &options, const std::filesystem::path& db_path)
-> Result<Storage> {
    rocksdb::DB* db = nullptr;
    rocksdb::Status status = rocksdb::DB::Open(options, db_path, &db);

    if (!status.ok()) {
        LOG_ERROR("{}", status.ToString());
        return std::unexpected{make_error(Error::kRocksDBFileOpenFailed)};
    }

    return Storage{db};
}

void foskv::storage::Storage::SetWriteOptions(const rocksdb::WriteOptions &write_options) {
    write_options_ = write_options;
}

void foskv::storage::Storage::SetReadOptions(const rocksdb::ReadOptions &read_options) {
    read_options_ = read_options;
}

auto foskv::storage::Storage::Put(const rocksdb::Slice& key, const rocksdb::Slice& value)
const -> rocksdb::Status {
    return db_->Put(write_options_, key, value);
}

auto foskv::storage::Storage::Get(const rocksdb::Slice& key, std::string *value)
const -> rocksdb::Status {
    return db_->Get(read_options_, key, value);
}

auto foskv::storage::Storage::Delete(const rocksdb::Slice& key)
const -> rocksdb::Status {
    return db_->Delete(write_options_, key);
}

auto foskv::storage::Storage::BatchWrite(rocksdb::WriteBatch& write_batch)
const -> rocksdb::Status {
    return db_->Write(write_options_, &write_batch);
}

auto foskv::storage::Storage::MultiGet(const std::vector<rocksdb::Slice> &keys,
    std::vector<std::string> &values) const -> std::vector<rocksdb::Status> {
    return db_->MultiGet(read_options_, keys, &values);
}
