#pragma once
#include "foskv/rpc/service.hpp"
#include "foskv/rpc/config.hpp"
#include "foskv/rpc/error.hpp"
#include "foskv/common/error.hpp"
#include "foskv/common/util/noncopyable.hpp"
#include "foskv/common/util/spsc_queue.hpp"
#include <kosio/net.hpp>
#include <kosio/core.hpp>
#include <tbb/concurrent_hash_map.h>

namespace foskv::rpc {
namespace detail {
#pragma pack(push, 1)
    struct FixedRequestHeader {
        uint64_t    request_id{0};
        ServiceType service_type{0};
        MethodType  method_type{0};
        uint32_t    payload_size{0};
    };

    struct FixedResponseHeader {
        uint64_t request_id{0};
        uint32_t payload_size{0};
    };
#pragma pack(pop)
} // namespace detail

class RpcType {
public:
    [[nodiscard]]
    static auto to_string(ServiceType service_type) -> std::string_view {
        switch (service_type) {
            case ServiceType::kRaft:
                return "Raft";
            case ServiceType::kKv:
                return "KV";
            default:
                return "Unknown";
        }
    }

    [[nodiscard]]
    static auto to_string(MethodType method_type) -> std::string_view {
        switch (method_type) {
            case MethodType::kRaftRequestVote:
                return "RaftRequestVote";
            case MethodType::kRaftAppendEntries:
                return "RaftAppendEntries";
            case MethodType::kRaftInstallSnapshot:
                return "RaftInstallSnapshot";
            case MethodType::kKvPut:
                return "KvPut";
            case MethodType::kKvGet:
                return "KvGet";
            case MethodType::kKvDelete:
                return "KvDelete";
            default:
                return "Unknown";
        }
    }
};
} // namespace foskv::rpc