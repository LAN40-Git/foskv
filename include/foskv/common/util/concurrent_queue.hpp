#pragma once
#include "foskv/common/error.hpp"
#include <kosio/sync.hpp>

namespace foskv {
template <typename T>
class ConcurrentQueue {
public:
    ConcurrentQueue() = default;
    ~ConcurrentQueue() = default;
public:
    ConcurrentQueue(const ConcurrentQueue&) = delete;
    ConcurrentQueue& operator=(const ConcurrentQueue&) = delete;
    ConcurrentQueue(ConcurrentQueue&&) = delete;
    ConcurrentQueue& operator=(ConcurrentQueue&&) = delete;
public:
    void shutdown() {
        is_shutdown_.store(true);
        cv_.notify_all();
    }

    [[REMEMBER_CO_AWAIT]]
    auto push(T value) -> kosio::async::Task<> {
        queue_.enqueue(std::move(value));
        cv_.notify_one();
        co_return;
    }

    [[REMEMBER_CO_AWAIT]]
    auto pop() -> kosio::async::Task<Result<T>> {
        T value;
        if (queue_.try_dequeue(value)) {
            co_return value;
        }
        co_await mutex_.lock();
        std::unique_lock lock(mutex_, std::adopt_lock);
        while (!queue_.try_dequeue(value)) {
            if (is_shutdown_) {
                co_return std::unexpected{make_error(Error::kEmptyConcurrentQueue)};
            }
            co_await cv_.wait(mutex_,
                [this] {
                    return is_shutdown_ ||
                        queue_.size_approx() != 0;
            });
        }
        co_return value;
    }

    void push_sync(T value) {
        queue_.enqueue(std::move(value));
    }

private:
    moodycamel::ConcurrentQueue<T> queue_;
    kosio::sync::Mutex             mutex_;
    kosio::sync::ConditionVariable cv_;
    std::atomic<bool>              is_shutdown_{false};
};
} // namespace foskv