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
        kRepeatedPeer,
        kLocalNodeNotFound,
        kInvalidRpcServerAddress,
        kInvalidPeerAddress,
        kInvalidLocalAddress,
        kInvalidDataDirectory,
        kInvalidLogIndex,
        kPersistStateGetFailed,
        kFirstLogIndexPutFailed,
        kLastLogIndexPutFailed,
        kLogIndexToUllFailed,
        kLogEntriesRecoverFailed,
        kPersistStateParseFailed,
        kPersistStateSerializeFailed,
        kLogEntryParseFailed,
        kLogEntrySerializeFailed,
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
        kLogEntriesPersistFailed,
        kLogEntriesTruncateFailed,
        kStatePersistFailed,
        kKVPutFailed,
    };

public:
    explicit Error(int error_code) : error_code_{error_code} {}

public:
    [[nodiscard]]
        auto message() const noexcept -> std::string_view {
        switch (error_code_) {
            case kUnknown:
                return "Unknown error.";
            case kLocalNodeNotFound:
                return "Local node not found in raft config file.";
            case kInvalidRpcServerAddress:
                return "Invalid rpc server address.";
            case kInvalidPeerAddress:
                return "Invalid peer address.";
            case kInvalidLocalAddress:
                return "Invalid local address.";
            case kInvalidDataDirectory:
                return "Invalid data directory.";
            case kInvalidLogIndex:
                return "Invalid first or last log index.";
            case kPersistStateGetFailed:
                return "Persist state get failed.";
            case kFirstLogIndexPutFailed:
                return "First log index put failed.";
            case kLastLogIndexPutFailed:
                return "Last log index put failed.";
            case kLogIndexToUllFailed:
                return "Log index to ull failed.";
            case kLogEntriesRecoverFailed:
                return "Log entries recover failed.";
            case kPersistStateParseFailed:
                return "Persist state parse failed.";
            case kPersistStateSerializeFailed:
                return "Persist state serialize failed.";
            case kLogEntryParseFailed:
                return "Log entry parse failed.";
            case kLogEntrySerializeFailed:
                return "Log entry serialize failed.";
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