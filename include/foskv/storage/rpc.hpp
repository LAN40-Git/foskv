#pragma once
#include <string_view>

namespace foskv::storage {
class KVRpc {
public:
#ifdef ENABLE_HUMAN_READABLE_RPC_NAMES
    static constexpr std::string_view ServiceName = "KVService";
    static constexpr std::string_view Put = "Put";
    static constexpr std::string_view Get = "Get";
    static constexpr std::string_view Delete = "Delete";
#else
    static constexpr std::string_view ServiceName = "K";
    static constexpr std::string_view Put = "K0";
    static constexpr std::string_view Get = "K1";
    static constexpr std::string_view Delete = "K2";
#endif
};
} // namespace foskv::storage