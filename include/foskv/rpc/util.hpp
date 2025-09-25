#pragma once
#include "foskv/rpc/pb/rpc.pb.h"
#include "foskv/rpc/config.hpp"
#include "foskv/rpc/error.hpp"
#include "foskv/common/error.hpp"
#include "foskv/common/util/noncopyable.hpp"
#include "foskv/common/util/concurrentqueue.hpp"
#include <xxhash.h>
#include <kosio/net.hpp>
#include <kosio/core.hpp>
#include <tbb/concurrent_hash_map.h>

namespace foskv::rpc::detail {
#pragma pack(push, 1)
struct FixedRequestHeader {
    uint64_t request_id{0};
    uint32_t header_size{0};
    uint32_t payload_size{0};
};

struct FixedResponseHeader {
    uint64_t request_id{0};
    uint32_t payload_size{0};
};
#pragma pack(pop)

struct SocketAddrXXHash {
    std::size_t operator()(const kosio::net::SocketAddr& addr) const noexcept {
        thread_local XXH64_state_t* state = XXH64_createState();
        XXH64_reset(state, 0);

        int family = addr.family();
        XXH64_update(state, &family, sizeof(family));

        uint16_t port = addr.port();
        XXH64_update(state, &port, sizeof(port));

        auto ip = addr.ip();
        if (const kosio::net::Ipv4Addr* ipv4 = std::get_if<kosio::net::Ipv4Addr>(&ip)) {
            uint32_t ip_val = ipv4->addr();
            XXH64_update(state, &ip_val, sizeof(ip_val));
        } else if (const kosio::net::Ipv6Addr* ipv6 = std::get_if<kosio::net::Ipv6Addr>(&ip)) {
            const in6_addr& in6 = ipv6->addr();
            XXH64_update(state, &in6, sizeof(in6));
        }

        auto hash = XXH64_digest(state);
        XXH64_freeState(state);
        return hash;
    }
};
} // namespace foskv::rpc::detail