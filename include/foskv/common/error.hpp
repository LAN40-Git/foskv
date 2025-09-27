#pragma once
#include <expected>
#include <string_view>
#include <cstring>
#include <format>

namespace foskv {
class Error {
public:
    enum ErrorCode {
        kUnknown = 8000,
        kProviderShutdown,
        kConsumerShutdown,
        kEmptySPSCQueue,
        kRepeatedPeer,
        kPeerNotFound,
        kLocalNodeNotFound,
        kInvalidRpcServerAddress,
        kInvalidRedirectAddress,
        kInvalidPeerAddress,
        kInvalidLocalAddress,
        kInvalidDataDirectory,
        kInvalidLogIndex,
        kTcpListenerBindFailed,
        kTcpListenerAcceptFailed,
        kTcpStreamCloseFailed,
        kFirstLogIndexPutFailed,
        kLastLogIndexPutFailed,
        kLogIndexToUllFailed,
        kLogEntriesRecoverFailed,
        kLogEntryParseFailed,
        kLogEntrySerializeFailed,
        kPersistStatePutFailed,
        kPersistStateGetFailed,
        kPersistStateParseFailed,
        kPersistStateSerializeFailed,
        kRequestVoteRequestRequestParseFailed,
        kRequestVoteRequestRequestSerializeFailed,
        kRequestVoteResponseParseFailed,
        kRequestVoteResponseSerializeFailed,
        kAppendEntriesRequestParseFailed,
        kAppendEntriesRequestSerializeFailed,
        kAppendEntriesResponseParseFailed,
        kAppendEntriesResponseSerializeFailed,
        kInstallSnapshotRequestParseFailed,
        kInstallSnapshotRequestSerializeFailed,
        kInstallSnapshotResponseParseFailed,
        kInstallSnapshotResponseSerializeFailed,
        kKVPutRequestParseFailed,
        kKVPutRequestSerializeFailed,
        kKVPutResponseParseFailed,
        kKVPutResponseSerializeFailed,
        kKVGetRequestParseFailed,
        kKVGetRequestSerializeFailed,
        kKVGetResponseParseFailed,
        kKVGetResponseSerializeFailed,
        kKVDeleteRequestParseFailed,
        kKVDeleteRequestSerializeFailed,
        kKVDeleteResponseParseFailed,
        kKVDeleteResponseSerializeFailed,
        kConnectionEffective,
        kConnectRpcServerFailed,
        kRaftConfigFileOpenFailed,
        kRaftConfigFileReadFailed,
        kRaftConfigFileWriteFailed,
        kRaftConfigFileRenameFailed,
        kTempRaftConfigFileOpenFailed,
        kTempRaftConfigFileReOpenFailed,
        kTempRaftConfigFileReadFailed,
        kTempRaftConfigFileWriteFailed,
        kTempRaftConfigFileRenameFailed,
        kRocksDBFileOpenFailed,
        kLogEntryPersistFailed,
        kLogEntriesPersistFailed,
        kLogEntriesTruncateFailed,
        kStatePersistFailed,
    };

public:
    explicit Error(int error_code) : error_code_{error_code} {}

public:
    [[nodiscard]]
        auto message() const noexcept -> std::string_view {
        switch (error_code_) {
            case kUnknown:
                return "Unknown error.";
            case kProviderShutdown:
                return "Provider has been shutdown.";
            case kConsumerShutdown:
                return "Consumer has been shutdown.";
            case kEmptySPSCQueue:
                return "Empty spsc queue.";
            case kRepeatedPeer:
                return "Repeated peer.";
            case kPeerNotFound:
                return "Peer not found.";
            case kLocalNodeNotFound:
                return "Local node not found in raft config file.";
            case kInvalidRpcServerAddress:
                return "Invalid rpc server address.";
            case kInvalidRedirectAddress:
                return "Invalid redirect address.";
            case kInvalidPeerAddress:
                return "Invalid peer address.";
            case kInvalidLocalAddress:
                return "Invalid local address.";
            case kInvalidDataDirectory:
                return "Invalid data directory.";
            case kInvalidLogIndex:
                return "Invalid first or last log index.";
            case kTcpListenerBindFailed:
                return "Tcp listener bind failed.";
            case kTcpListenerAcceptFailed:
                return "Tcp listener accept failed.";
            case kTcpStreamCloseFailed:
                return "Tcp stream close failed.";
            case kFirstLogIndexPutFailed:
                return "First log index put failed.";
            case kLastLogIndexPutFailed:
                return "Last log index put failed.";
            case kLogIndexToUllFailed:
                return "Log index to ull failed.";
            case kLogEntriesRecoverFailed:
                return "Log entries recover failed.";
            case kLogEntryParseFailed:
                return "Log entry parse failed.";
            case kLogEntrySerializeFailed:
                return "Log entry serialize failed.";
            case kPersistStateGetFailed:
                return "Persist state get failed.";
            case kPersistStatePutFailed:
                return "Persist state put failed.";
            case kPersistStateParseFailed:
                return "Persist state parse failed.";
            case kPersistStateSerializeFailed:
                return "Persist state serialize failed.";
            case kRequestVoteRequestRequestParseFailed:
                return "Request vote request parse failed.";
            case kRequestVoteRequestRequestSerializeFailed:
                return "Request vote request serialize failed.";
            case kRequestVoteResponseParseFailed:
                return "Request vote response parse failed.";
            case kRequestVoteResponseSerializeFailed:
                return "Request vote response serialize failed.";
            case kAppendEntriesRequestParseFailed:
                return "Append entries request parse failed.";
            case kAppendEntriesRequestSerializeFailed:
                return "Append entries request serialize failed.";
            case kAppendEntriesResponseParseFailed:
                return "Append entries response parse failed.";
            case kAppendEntriesResponseSerializeFailed:
                return "Append entries response serialize failed.";
            case kInstallSnapshotRequestParseFailed:
                return "Install snapshot request parse failed.";
            case kInstallSnapshotRequestSerializeFailed:
                return "Install snapshot request serialize failed.";
            case kInstallSnapshotResponseParseFailed:
                return "Install snapshot response parse failed.";
            case kInstallSnapshotResponseSerializeFailed:
                return "Install snapshot response serialize failed.";
            case kKVPutRequestParseFailed:
                return "KVPut request parse failed.";
            case kKVPutRequestSerializeFailed:
                return "KVPut request serialize failed.";
            case kKVPutResponseParseFailed:
                return "KVPut response parse failed.";
            case kKVPutResponseSerializeFailed:
                return "KVPut response serialize failed.";
            case kKVGetRequestParseFailed:
                return "KVGet request parse failed.";
            case kKVGetRequestSerializeFailed:
                return "KVGet request serialize failed.";
            case kKVGetResponseParseFailed:
                return "KVGet response parse failed.";
            case kKVGetResponseSerializeFailed:
                return "KVGet response serialize failed.";
            case kKVDeleteRequestParseFailed:
                return "KVDelete request parse failed.";
            case kKVDeleteRequestSerializeFailed:
                return "KVDelete request serialize failed.";
            case kKVDeleteResponseParseFailed:
                return "KVDelete response parse failed.";
            case kKVDeleteResponseSerializeFailed:
                return "KVDelete response serialize failed.";
            case kConnectionEffective:
                return "Failed to connect to rpc server, connection is effective.";
            case kConnectRpcServerFailed:
                return "Failed to connect to rpc server.";
            case kRaftConfigFileOpenFailed:
                return "Failed to open raft config file.";
            case kRaftConfigFileReadFailed:
                return "Failed to read raft config file.";
            case kRaftConfigFileWriteFailed:
                return "Failed to write raft config file.";
            case kRaftConfigFileRenameFailed:
                return "Failed to rename raft config file.";
            case kTempRaftConfigFileOpenFailed:
                return "Failed to open temp raft config file.";
            case kTempRaftConfigFileReOpenFailed:
                return "Failed to reopen temp raft config file.";
            case kTempRaftConfigFileReadFailed:
                return "Failed to read temp raft config file.";
            case kTempRaftConfigFileWriteFailed:
                return "Failed to write temp raft config file.";
            case kTempRaftConfigFileRenameFailed:
                return "Failed to rename temp raft config file.";
            case kRocksDBFileOpenFailed:
                return "Failed to open RocksDB file.";
            case kLogEntryPersistFailed:
                return "Failed to persist log entry.";
            case kLogEntriesPersistFailed:
                return "Failed to persist log entries.";
            case kLogEntriesTruncateFailed:
                return "Failed to truncate log entries.";
            case kStatePersistFailed:
                return "Failed to persist state.";
            default:
                return strerror(error_code_);
        }
    }

    [[nodiscard]]
    auto value() const noexcept -> int {
        return error_code_;
    }

private:
    int error_code_;
};

template <typename T>
using Result = std::expected<T, Error>;

static auto make_error(int error_code) -> Error {
    return Error{error_code};
}
} // namespace foskv

namespace std {
template <>
struct formatter<foskv::Error> {
public:
    constexpr auto parse(format_parse_context &context) {
        auto it{context.begin()};
        auto end{context.end()};
        if (it == end || *it == '}') {
            return it;
        }
        ++it;
        if (it != end && *it != '}') {
            throw format_error("Invalid format specifier for Error");
        }
        return it;
    }

    auto format(const foskv::Error &error, auto &context) const noexcept {
        return format_to(context.out(), "{} (error {})", error.message(), error.value());
    }
};
} // namespace std