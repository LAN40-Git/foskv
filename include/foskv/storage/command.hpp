#pragma once
#include "foskv/common/macros.hpp"
#include <kosio/async/coroutine/task.hpp>
#include <kosio/io/stdio.hpp>
#include <string>
#include <string_view>

namespace foskv::storage {
class KVCommand {
private:
    static constexpr std::string_view OP_PUT = "Put ";
    static constexpr std::string_view OP_GET = "Get ";
    static constexpr std::string_view OP_DELETE = "Delete ";

public:
    enum class Op {
        kUnknown = 0,
        kPut,
        kGet,
        kDelete,
    };

    struct Args {
        Op          op;
        std::string key;
        std::string value;
    };

public:
    static auto parse(std::string_view cmd) -> Args {
        // 忽略换行符
        if (auto pos = cmd.find('\n'); pos != std::string_view::npos) {
            cmd = cmd.substr(0, pos);
        }

        if (cmd.starts_with(OP_PUT)) {
            return parse_put(cmd);
        } else if (cmd.starts_with(OP_GET)) {
            return parse_get(cmd);
        } else if (cmd.starts_with(OP_DELETE)) {
            return parse_delete(cmd);
        }

        return Args{Op::kUnknown, "", ""};
    }

    static auto async_parse() -> kosio::async::Task<Args> {
        thread_local char CMD[1024]{};
        // 从标准输入读取命令
        co_await kosio::io::input(CMD);
        std::string_view cmd{CMD};
        co_return parse(cmd);
    }

private:
    static auto parse_put(std::string_view cmd) -> Args {
        auto first_space_pos = cmd.find_first_of(' ');
        auto second_space_pos = cmd.find_last_of(' ');
        if (first_space_pos == std::string_view::npos ||
            second_space_pos == std::string_view::npos ||
            first_space_pos == second_space_pos) {
            return Args{Op::kUnknown, "", ""};
        }

        auto key = cmd.substr(first_space_pos + 1, second_space_pos - first_space_pos - 1);
        auto value = cmd.substr(second_space_pos + 1);
        if (key.empty() || value.empty()) {
            return Args{Op::kUnknown, "", ""};
        }

        return Args{Op::kPut, std::string{key}, std::string{value}};
    }

    static auto parse_get(std::string_view cmd) -> Args {
        auto first_space_pos = cmd.find_first_of(' ');
        auto key = cmd.substr(first_space_pos + 1);
        if (key.empty()) {
            return Args{Op::kUnknown, "", ""};
        }
        return Args{Op::kGet, std::string{key}, ""};
    }

    static auto parse_delete(std::string_view cmd) -> Args {
        auto first_space_pos = cmd.find_first_of(' ');
        auto key = cmd.substr(first_space_pos + 1);
        if (key.empty()) {
            return Args{Op::kUnknown, "", ""};
        }
        return Args{Op::kDelete, std::string{key}, ""};
    }
};
} // namespace foskv::storage