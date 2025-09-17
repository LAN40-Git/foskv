#include "foskv/storage/storage.hpp"
#include <kosio/common/debug.hpp>

auto foskv::storage::KVStorage::Open(const std::string &db_path) -> StorageResult<KVStorage> {
    rocksdb::Options options;
    options.create_if_missing = true;
    options.error_if_exists = false;
    options.blob_cache = rocksdb::NewLRUCache(64 * 1024 * 1024);
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();

    rocksdb::DB* db = nullptr;
    rocksdb::Status status = rocksdb::DB::Open(options, db_path, &db);

    if (!status.ok()) {
        return std::unexpected{make_storage_error(StorageError::kDBOpenFailed)};
    }

    return KVStorage(db);
}

auto foskv::storage::KVStorage::Put(std::string_view key, std::string_view value) const
    -> rocksdb::Status {
    rocksdb::Slice key_slice(key.data(), key.size());
    rocksdb::Slice value_slice(value.data(), value.size());
    return db_->Put(rocksdb::WriteOptions(), key_slice, value_slice);
}

auto foskv::storage::KVStorage::Get(std::string_view key, std::string *value) const
    -> rocksdb::Status {
    rocksdb::Slice key_slice(key.data(), key.size());
    return db_->Get(rocksdb::ReadOptions(), key_slice, value);
}

auto foskv::storage::KVStorage::Delete(std::string_view key) const
    -> rocksdb::Status {
    rocksdb::Slice key_slice(key.data(), key.size());
    auto status = db_->Delete(rocksdb::WriteOptions(), key_slice);
    return status;
}

auto foskv::storage::KVStorage::RpcPut(std::string_view req_payload, std::span<char> resp_payload) const -> RpcResult<std::size_t> {
    PutRequest req;
    PutResponse resp;
    std::size_t resp_payload_size;

    auto* resp_header = resp.mutable_header();
    if (!req.ParseFromArray(req_payload.data(), req_payload.size())) {
        resp_header->set_success(false);
        resp_header->set_error("Invalid put request");
        resp_payload_size = resp.ByteSizeLong();
        if (!resp.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
            return std::unexpected{make_rpc_error(RpcError::kSerializeFailed)};
        }
        return resp_payload_size;
    }

    auto key = req.key();
    auto value = req.value();
    auto status = Put(key, value);
    if (!status.ok()) {
        resp_header->set_success(false);
        resp_header->set_error(status.ToString());
        resp_payload_size = resp.ByteSizeLong();
        if (!resp.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
            return std::unexpected{make_rpc_error(RpcError::kSerializeFailed)};
        }
        return resp_payload_size;
    }

    resp_header->set_success(true);
    resp_payload_size = resp.ByteSizeLong();
    if (!resp.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
        return std::unexpected{make_rpc_error(RpcError::kSerializeFailed)};
    }
    return resp_payload_size;
}

auto foskv::storage::KVStorage::RpcGet(std::string_view req_payload, std::span<char> resp_payload) const -> RpcResult<std::size_t> {
    GetRequest req;
    GetResponse resp;
    std::size_t resp_payload_size;

    auto* resp_header = resp.mutable_header();
    if (!req.ParseFromArray(req_payload.data(), req_payload.size())) {
        resp_header->set_success(false);
        resp_header->set_error("Invalid get request");
        resp_payload_size = resp.ByteSizeLong();
        if (!resp.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
            return std::unexpected{make_rpc_error(RpcError::kSerializeFailed)};
        }
        return resp_payload_size;
    }

    auto key = req.key();
    auto status = Get(key, resp.mutable_value());
    if (!status.ok()) {
        resp_header->set_success(false);
        resp_header->set_error(status.ToString());
        resp_payload_size = resp.ByteSizeLong();
        if (!resp.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
            return std::unexpected{make_rpc_error(RpcError::kSerializeFailed)};
        }
        return resp_payload_size;
    }

    resp_header->set_success(true);
    resp_payload_size = resp.ByteSizeLong();
    if (!resp.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
        return std::unexpected{make_rpc_error(RpcError::kSerializeFailed)};
    }
    return resp_payload_size;
}

auto foskv::storage::KVStorage::RpcDelete(std::string_view req_payload, std::span<char> resp_payload) const -> RpcResult<std::size_t> {
    DeleteRequest req;
    DeleteResponse resp;
    std::size_t resp_payload_size;

    auto* resp_header = resp.mutable_header();
    if (!req.ParseFromArray(req_payload.data(), req_payload.size())) {
        resp_header->set_success(false);
        resp_header->set_error("Invalid delete request");
        resp_payload_size = resp.ByteSizeLong();
        if (!resp.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
            return std::unexpected{make_rpc_error(RpcError::kSerializeFailed)};
        }
        return resp_payload_size;
    }

    auto key = req.key();
    auto status = Delete(key);
    if (!status.ok()) {
        resp_header->set_success(false);
        resp_header->set_error(status.ToString());
        resp_payload_size = resp.ByteSizeLong();
        if (!resp.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
            return std::unexpected{make_rpc_error(RpcError::kSerializeFailed)};
        }
        return resp_payload_size;
    }

    resp_header->set_success(true);
    resp_payload_size = resp.ByteSizeLong();
    if (!resp.SerializeToArray(resp_payload.data(), resp_payload_size)) [[unlikely]] {
        return std::unexpected{make_rpc_error(RpcError::kSerializeFailed)};
    }
    return resp_payload_size;
}

auto foskv::storage::KVStorage::BatchWrite(const std::vector<std::pair<std::string, std::string>> &kvs) const
    -> rocksdb::Status {
    rocksdb::WriteBatch batch;

    for (const auto& [key, value] : kvs) {
        batch.Put(key, value);
    }

    return db_->Write(rocksdb::WriteOptions(), &batch);
}

auto foskv::storage::KVStorage::MultiGet(
    const std::vector<std::string_view> &keys,
    std::vector<std::string> *values) const -> std::vector<rocksdb::Status> {
    std::vector<rocksdb::Slice> key_slices;
    key_slices.reserve(keys.size());
    for (auto key : keys) {
        key_slices.emplace_back(key.data(), key.size());
    }

    values->resize(keys.size());
    return db_->MultiGet(rocksdb::ReadOptions(),
                        key_slices,
                        values);
}
