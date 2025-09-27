#pragma once
#include "foskv/common/error.hpp"
#include <kosio/sync.hpp>

namespace foskv::util {
template <typename T>
class SPSCQueue {
public:
    [[REMEMBER_CO_AWAIT]]
    auto push(T&& value) -> kosio::async::Task<> {
        co_await mutex_.lock();
        std::unique_lock lock{mutex_, std::adopt_lock};
        queue_.push(std::move(value));
        cv_.notify_one();
    }

    [[REMEMBER_CO_AWAIT]]
    auto pop() -> kosio::async::Task<Result<T>> {
        co_await mutex_.lock();
        std::unique_lock lock{mutex_, std::adopt_lock};
        while (queue_.empty()) {
            co_await cv_.wait(mutex_, [this]() {
                return !queue_.empty() || is_shutdown_.load(std::memory_order_relaxed);
            });
            if (is_shutdown_.load(std::memory_order_relaxed)) {
                co_return std::unexpected{make_error(Error::kEmptySPSCQueue)};
            }
        }
        T item = std::move(queue_.front());
        queue_.pop();
        co_return item;
    }

    [[REMEMBER_CO_AWAIT]]
    auto push_batch(std::vector<T> items) -> kosio::async::Task<> {
        co_await mutex_.lock();
        std::unique_lock lock{mutex_, std::adopt_lock};
        for (auto& item : items) {
            queue_.push(std::move(item));
        }
    }

    [[REMEMBER_CO_AWAIT]]
    auto pop_all() -> kosio::async::Task<std::vector<T>> {
        co_await mutex_.lock();
        std::unique_lock lock{mutex_, std::adopt_lock};

        std::vector<T> items;
        items.reserve(queue_.size());

        while (!queue_.empty()) {
            items.push_back(std::move(queue_.front()));
            queue_.pop();
        }

        co_return std::move(items);
    }

    [[nodiscard]]
    auto size() -> std::size_t {
        return queue_.size();
    }

    [[REMEMBER_CO_AWAIT]]
    auto shutdown() -> kosio::async::Task<> {
        co_await mutex_.lock();
        std::unique_lock lock{mutex_, std::adopt_lock};
        is_shutdown_.store(true, std::memory_order_relaxed);
        cv_.notify_all();
    }

    [[REMEMBER_CO_AWAIT]]
    auto run() -> kosio::async::Task<> {
        co_await mutex_.lock();
        std::unique_lock lock{mutex_, std::adopt_lock};
        is_shutdown_.store(false, std::memory_order_relaxed);
    }

private:
    std::queue<T>                  queue_;
    kosio::sync::Mutex             mutex_;
    kosio::sync::ConditionVariable cv_;
    std::atomic<bool>              is_shutdown_{false};
};
} // namespace foskv::util