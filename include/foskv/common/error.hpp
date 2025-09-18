#pragma once
#include <expected>
#include <string_view>
#include <cstring>
#include <format>
#include <kosio/common/error.hpp>

namespace foskv {
namespace detail {
static inline constexpr int ErrorCodeBase = 8000;
static inline constexpr int ErrorCodeInterval = 1000;
// Rpc Error
static inline constexpr int RpcErrorCodeBase = ErrorCodeBase;
// Storage Error
static inline constexpr int StorageErrorCodeBase = RpcErrorCodeBase + ErrorCodeInterval;
// Raft Error
static inline constexpr int RaftErrorCodeBase = StorageErrorCodeBase + ErrorCodeInterval;

template <class DeriverError>
class BaseError {
public:
    explicit BaseError(int error_code)
        : error_code_(error_code) {}

public:
    [[nodiscard]]
    auto value() const noexcept -> int { return error_code_; }

    [[nodiscard]]
    auto message() const noexcept -> std::string_view {
        return static_cast<const DeriverError*>(this)->error_message();
    }

protected:
    int error_code_;
};
} // namespace detail

/* Template of DriverError
// ========== Fixme ==========
class Fixme : public detail::BaseError<Fixme> {
public:
    enum Code {
        kUnknown = Fixme,
    };

public:
    explicit Fixme(int error_code)
        : BaseError<Fixme>(error_code) {}

public:
    [[nodiscard]]
    auto error_message() const noexcept -> std::string_view {
        switch (static_cast<Code>(error_code_)) {
            case kUnknown:
                return "Unknown Fixme error.";
            default:
                return strerror(error_code_);
        }
    }
};
*/

// ========== Rpc Error ==========
class RpcError : public detail::BaseError<RpcError> {
public:
    enum Code {
        kUnknown = detail::RpcErrorCodeBase,
        kFdNotRegister,
        kSerializeFailed,
        kParseFailed,
        kReconnectFailed,
        kSendFailed,
        kReceiveFailed,
        kMessageTooLarge,
    };

public:
    explicit RpcError(Code error_code)
        : BaseError<RpcError>(static_cast<int>(error_code)) {}

public:
    [[nodiscard]]
    auto error_message() const noexcept -> std::string_view {
        switch (static_cast<Code>(error_code_)) {
            case kUnknown:
                return "Unknown rpc error.";
            case kFdNotRegister:
                return "Fd not register.";
            case kSerializeFailed:
                return "Failed to serialize message.";
            case kParseFailed:
                return "Failed to parse message.";
            case kReconnectFailed:
                return "Failed to reconnect to the rpc server.";
            case kSendFailed:
                return "Failed to send message.";
            case kReceiveFailed:
                return "Failed to receive message.";
            case kMessageTooLarge:
                return "Message too large.";
            default:
                return strerror(error_code_);
        }
    }
};

// ========== Storage Error ==========
class StorageError : public detail::BaseError<StorageError> {
public:
    enum Code {
        kUnknown = detail::StorageErrorCodeBase,
        kDBOpenFailed,
    };
public:
    explicit StorageError(Code error_code)
        : BaseError<StorageError>(static_cast<int>(error_code)) {}

public:
    [[nodiscard]]
    auto error_message() const noexcept -> std::string_view {
        switch (static_cast<Code>(error_code_)) {
            case kUnknown:
                return "Unknown storage error.";
            case kDBOpenFailed:
                return "Failed to open database.";
            default:
                return strerror(error_code_);
        }
    }
};

// ========== Raft Error ==========
class RaftError : public detail::BaseError<RaftError> {
public:
    enum Code {
        kUnknown = detail::RaftErrorCodeBase,
        kConfigFileOpenFailed,
        kPersisterCreateFailed,
        kPersistentSaveFailed,
        kInvalidPeerAddress,
        kRepeatedPeer,
        kJsonParseFailed,
        kLocalNodeNotFound,
    };

public:
    explicit RaftError(Code error_code)
        : BaseError<RaftError>(static_cast<int>(error_code)) {}

public:
    [[nodiscard]]
    auto error_message() const noexcept -> std::string_view {
        switch (static_cast<Code>(error_code_)) {
            case kUnknown:
                return "Unknown raft error.";
            case kConfigFileOpenFailed:
                return "Failed to open configuration file.";
            case kPersisterCreateFailed:
                return "Failed to create persister.";
            case kPersistentSaveFailed:
                return "Failed to save persistent.";
            case kInvalidPeerAddress:
                return "Invalid peer address.";
            case kRepeatedPeer:
                return "Repeated peer.";
            case kJsonParseFailed:
                return "Failed to parse JSON.";
            case kLocalNodeNotFound:
                return "Failed to find local node in configuration.";
            default:
                return strerror(error_code_);
        }
    }
};

namespace detail {
template <class ErrorType>
    requires std::derived_from<ErrorType, BaseError<ErrorType>>
[[nodiscard]]
static inline auto make_error(int error_code) -> BaseError<ErrorType> {
    return detail::BaseError<ErrorType>(error_code);
}

template <typename ResultType, class ErrorType>
using Result = std::expected<ResultType, BaseError<ErrorType>>;
} // namespace detail

template <class ResultType>
using RpcResult = detail::Result<ResultType, RpcError>;
[[nodiscard]]
static inline auto make_rpc_error(int error_code) ->detail::BaseError<RpcError> {
    return detail::make_error<RpcError>(error_code);
}

template <class ResultType>
using StorageResult = detail::Result<ResultType, StorageError>;
[[nodiscard]]
static inline auto make_storage_error(int error_code) ->detail::BaseError<StorageError> {
    return detail::make_error<StorageError>(error_code);
}

template <class ResultType>
using RaftResult = detail::Result<ResultType, RaftError>;
[[nodiscard]]
static inline auto make_raft_error(int error_code) ->detail::BaseError<RaftError> {
    return detail::make_error<RaftError>(error_code);
}
} // namespace foskv

namespace std {
template <typename ErrorType>
struct formatter<foskv::detail::BaseError<ErrorType>> {
public:
    constexpr auto parse(format_parse_context& ctx) {
        auto it = ctx.begin();
        auto end = ctx.end();
        if (it != end && *it != '}') {
            throw format_error("Invalid format specifier for Error");
        }
        return it;
    }

    auto format(const foskv::detail::BaseError<ErrorType>& error, auto& ctx) const {
        return format_to(ctx.out(), "{} (error {})", error.message(), error.value());
    }
};
} // namespace std