#pragma once
#include <string_view>
#include <fstream>
#include <string>
#include "foskv/common/error.hpp"
#include "foskv/common/util/noncopyable.hpp"

namespace foskv::raft {
class Persistent : util::Noncopyable {
public:
    static auto create(std::string_view path) -> RaftResult<Persistent>;

public:
    void save();

private:

};
} // namespace foskv::raft