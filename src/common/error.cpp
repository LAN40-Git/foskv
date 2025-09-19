#include "foskv/common/error.hpp"

auto foskv::RpcError::error_message() const noexcept -> std::string_view {
    switch (static_cast<Code>(error_code_)) {
        case kUnknown:
            return "Unknown rpc error.";
        case kFdNotRegister:
            return "Fd not register.";
        case kSerializeFailed:
            return "Failed to serialize message.";
        case kParseFailed:
            return "Failed to parse message.";
        case kConnectFailed:
            return "Failed to connect to rpc server.";
        case kReconnectFailed:
            return "Failed to reconnect to the rpc server.";
        case kSendFailed:
            return "Failed to send message.";
        case kReceiveFailed:
            return "Failed to receive message.";
        case kMessageTooLarge:
            return "Message too large.";
        case kOtherRaftCluster:
            return "Message from other raft cluster.";
        case kInvalidRpcServerAddress:
            return "Invalid provider address.";
        default:
            return strerror(error_code_);
    }
}

auto foskv::StorageError::error_message() const noexcept -> std::string_view {
    switch (static_cast<Code>(error_code_)) {
        case kUnknown:
            return "Unknown storage error.";
        case kDBOpenFailed:
            return "Failed to open database.";
        default:
            return strerror(error_code_);
    }
}

auto foskv::RaftError::error_message() const noexcept -> std::string_view {
    switch (static_cast<Code>(error_code_)) {
        case kUnknown:
            return "Unknown raft error.";
        case kConfigFileOpenFailed:
            return "Failed to open raft configuration file.";
        case kConfigFileWriteFailed:
            return "Failed to write raft configuration file.";
        case kConfigFileReadFailed:
            return "Failed to read raft configuration file.";
        case kConfigFileRenameFailed:
            return "Failed to rename raft configuration file.";
        case kPersisterCreateFailed:
            return "Failed to create persister.";
        case kPersistentSaveFailed:
            return "Failed to save persistent.";
        case kInvalidPeerAddress:
            return "Invalid peer raft node address.";
        case kInvalidLocalAddress:
            return "Invalid local raft node address.";
        case kRepeatedPeer:
            return "Find repeated peer in cluster.";
        case kJsonParseFailed:
            return "Failed to parse JSON.";
        case kLocalNodeNotFound:
            return "Failed to find local raft node in configuration.";
        case kStateMachineCreateFailed:
            return "Failed to create state machine.";
        case kPeerCreateFailed:
            return "Failed to create peer.";
        default:
            return strerror(error_code_);
    }
}

auto foskv::ClientError::error_message() const noexcept -> std::string_view {
    switch (static_cast<Code>(error_code_)) {
        case kUnknown:
            return "Unknown storage error.";
        case kConnectFailed:
            return "Failed to connect to server.";
        case kReconnectFailed:
            return "Failed to reconnect to server.";
        default:
            return strerror(error_code_);
    }
}
