#include "foskv/raft/raft_log.hpp"
using namespace foskv::raft;

constexpr std::string_view TEST_DIR = "test";

auto run() -> kosio::async::Task<> {
    auto has_log = detail::RaftLog::create(TEST_DIR);
    if (!has_log) {
        kosio::log::console.error("{}", has_log.error());
        co_return;
    }
    auto has_state = has_log.value().recover_state_test();
    if (!has_state) {
        kosio::log::console.error("{}", has_state.error());
    }
    auto logs = std::move(has_log.value());

    // PersistState
    kosio::log::console.info("current_term : {}, voted_for : {}",
        has_state.value().current_term(), has_state.value().voted_for());

    // LogEntries
    auto entries = logs.entries_test();
    for (auto& entry : entries) {
        kosio::log::console.info("index : {}, term : {}", entry.index(), entry.term());
    }

    // Append Entries
    entries.clear();
    for (uint64_t i = 1; i < 128; ++i) {
        LogEntry entry;
        entry.set_index(i);
        entry.set_term(i);
        entries.emplace_back(std::move(entry));
    }
    logs.append_entries(std::move(entries));

    // Persist state
    logs.persist_state_test(12, 88);
}

auto main() -> int {
    kosio::runtime::CurrentThreadBuilder::default_create().block_on(run());
}