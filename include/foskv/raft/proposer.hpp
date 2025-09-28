#pragma once
#include "foskv/raft/util.hpp"

namespace foskv::raft::detail {
class Proposer {
public:

public:
    void propose(LogEntry entry) {
        entries_.Add(std::move(entry));
    }

    [[nodiscard]]
    auto take() noexcept -> google::protobuf::RepeatedPtrField<LogEntry> {
        google::protobuf::RepeatedPtrField<LogEntry> entries;
        entries.Swap(&entries);
        entries_.Clear();
        return entries;
    }

    [[nodiscard]]
    auto size() const noexcept -> std::size_t {
        return entries_.size();
    }

private:

    google::protobuf::RepeatedPtrField<LogEntry> entries_;
};
} // namespace foskv::raft::detail