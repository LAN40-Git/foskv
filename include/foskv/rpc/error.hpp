#pragma once
#include <string_view>

namespace foskv::rpc {
class RpcError {
public:
    enum ErrorCode {
        kUnknown = 0,
        kNoError,
        kLogEntryAppendOrPersistFailed,
        kNeedRedirect,
        kKVPutFailed,
        kKVGetFailed,
        kKVDeleteFailed,
        kKVPutRequestParseFailed,
        kKVGetRequestParseFailed,
        kKVDeleteRequestParseFailed,
    };

public:
    explicit RpcError(int error_code)
        : error_code_(error_code) {}

public:
    [[nodiscard]]
    auto value() const noexcept -> int { return error_code_; }

    [[nodiscard]]
    auto message() const noexcept -> std::string_view {
        switch (error_code_) {
            case kUnknown:
                return "Unknown rpc error.";
            case kNoError:
                return "No error.";
            case kLogEntryAppendOrPersistFailed:
                return "Log entry append or persist failed, try again.";
            case kNeedRedirect:
                return "Need redirect to the raft leader.";
            case kKVPutFailed:
                return "Failed to put kv.";
            case kKVGetFailed:
                return "Failed to get kv.";
            case kKVDeleteFailed:
                return "Failed to delete kv.";
            case kKVPutRequestParseFailed:
                return "Failed to parse kv.";
            case kKVGetRequestParseFailed:
                return "Failed to parse kv.";
            case kKVDeleteRequestParseFailed:
                return "Failed to delete kv.";
            default:
                return "Unknown rpc error.";
        }
    }

private:
    int error_code_;
};
} // namespace foskv::rpc