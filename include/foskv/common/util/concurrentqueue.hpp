#pragma once
#include <kosio/sync.hpp>
#include <kosio/third_party/concurrentqueue/concurrentqueue.h>

namespace foskv::util {
template <typename T>
class ConcurrentQueue {
public:
    ConcurrentQueue() = default;
    ~ConcurrentQueue() = default;

    // Delete copy
    ConcurrentQueue(const ConcurrentQueue&) = delete;
    ConcurrentQueue& operator=(const ConcurrentQueue&) = delete;

    // Delete move
    ConcurrentQueue(ConcurrentQueue&&) = delete;
    ConcurrentQueue& operator=(ConcurrentQueue&&) = delete;

public:
    void push(T&& value) {
        queue_.enqueue(std::move(value));
        cv_.notify_one();
    }

    [[REMEMBER_CO_AWAIT]]
    auto pop() -> kosio::async::Task<Result<T>> {
        T item;
        if (queue_.try_dequeue(item)) {
            co_return item;
        }
        co_await mutex_.lock();
        std::unique_lock lock{mutex_, std::adopt_lock};
        while (!queue_.try_dequeue(item)) {
            co_await cv_.wait(mutex_, [this]() {
                return queue_.size_approx() != 0 || is_shutdown_.load(std::memory_order_relaxed);
            });
            if (is_shutdown_.load(std::memory_order_relaxed)) {
                co_return std::unexpected{make_error(Error::kEmptyConcurrentQueue)};
            }
        }
        co_return item;
    }

    void shutdown() {
        is_shutdown_.store(true, std::memory_order_relaxed);
        cv_.notify_all();
    }

private:
    moodycamel::ConcurrentQueue<T> queue_;
    kosio::sync::Mutex             mutex_;
    kosio::sync::ConditionVariable cv_;
    std::atomic<bool>              is_shutdown_{false};
};
} // namespace foskv::util