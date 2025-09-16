#pragma once
#include "foskv/common/error.hpp"
#include "foskv/rpc/rpc.pb.h"
#include <kosio/core.hpp>
#include <kosio/net.hpp>
#include <string>
#include <functional>

namespace foskv::rpc::detail {
using RpcCallback = std::function<kosio::async::Task<>(RpcResult<std::string_view> has_response)>;
} // namespace foskv::rpc::detail