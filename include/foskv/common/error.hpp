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
// Client Error
static inline constexpr int ClientErrorCodeBase = RaftErrorCodeBase + ErrorCodeInterval;
// KV Error
static inline constexpr int KVErrorCodeBase = ClientErrorCodeBase + ErrorCodeInterval;

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

// ========== Rpc Error ==========
class RpcError : public detail::BaseError<RpcError> {
public:
    enum Code {
        kUnknown = detail::RpcErrorCodeBase,
        kFdNotRegister,
        kSerializeFailed,
        kParseFailed,
        kConnectFailed,
        kReconnectFailed,
        kSendFailed,
        kReceiveFailed,
        kMessageTooLarge,
        kOtherRaftCluster,
        kInvalidRpcServerAddress,
        kEntryPersistFailed,
        kRpcServiceNotExists,
    };

public:
    explicit RpcError(Code error_code)
        : BaseError<RpcError>(static_cast<int>(error_code)) {}

public:
    [[nodiscard]]
    auto error_message() const noexcept -> std::string_view;
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
    auto error_message() const noexcept -> std::string_view;
};

// ========== Raft Error ==========
class RaftError : public detail::BaseError<RaftError> {
public:
    enum Code {
        kUnknown = detail::RaftErrorCodeBase,
        kConfigFileOpenFailed,
        kConfigFileWriteFailed,
        kConfigFileReadFailed,
        kConfigFileRenameFailed,
        kPersisterCreateFailed,
        kPersistentSaveFailed,
        kInvalidPeerAddress,
        kInvalidLocalAddress,
        kRepeatedPeer,
        kJsonParseFailed,
        kLocalNodeNotFound,
        kStateMachineCreateFailed,
        kPeerCreateFailed,
        kUnknownCommand,
        kCommandParseFailed,
    };

public:
    explicit RaftError(Code error_code)
        : BaseError<RaftError>(static_cast<int>(error_code)) {}

public:
    [[nodiscard]]
    auto error_message() const noexcept -> std::string_view;
};

// ========== Client Error ==========
class ClientError : public detail::BaseError<ClientError> {
public:
    enum Code {
        kUnknown = detail::ClientErrorCodeBase,
        kConnectFailed,
        kReconnectFailed,
    };
public:
    explicit ClientError(Code error_code)
        : BaseError<ClientError>(static_cast<int>(error_code)) {}

public:
    [[nodiscard]]
    auto error_message() const noexcept -> std::string_view;
};

// ========== Server Error ==========
class ServerError : public detail::BaseError<ServerError> {
public:
    enum Code {
        kUnknown = detail::ClientErrorCodeBase,
        kRaftNodeCreationFailed,
    };
public:
    explicit ServerError(Code error_code)
        : BaseError<ServerError>(static_cast<int>(error_code)) {}

public:
    [[nodiscard]]
    auto error_message() const noexcept -> std::string_view;
};

// ====== KV Error ======
class KVError : public detail::BaseError<KVError> {
public:
    enum Code {
        kUnknown = detail::KVErrorCodeBase,
        kPutFailed,
        kGetFailed,
        kDeleteFailed,
        kNotFound,
        kNeedRedirect,
    };
public:
    explicit KVError(Code error_code)
        : BaseError<KVError>(static_cast<int>(error_code)) {}

public:
    [[nodiscard]]
    auto error_message() const noexcept -> std::string_view;
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

template <class ResultType>
using ClientResult = detail::Result<ResultType, ClientError>;
[[nodiscard]]
static inline auto make_client_error(int error_code) ->detail::BaseError<ClientError> {
    return detail::make_error<ClientError>(error_code);
}

template <class ResultType>
using ServerResult = detail::Result<ResultType, ServerError>;
[[nodiscard]]
static inline auto make_server_error(int error_code) ->detail::BaseError<ServerError> {
    return detail::make_error<ServerError>(error_code);
}

template <class ResultType>
using KVResult = detail::Result<ResultType, KVError>;
[[nodiscard]]
static inline auto make_kv_error(int error_code) ->detail::BaseError<KVError> {
    return detail::make_error<KVError>(error_code);
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