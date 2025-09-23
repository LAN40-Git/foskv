#pragma once
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
    [[REMEMBER_CO_AWAIT]]
    auto push(T value) -> kosio::async::Task<> {
        queue_.enqueue(std::move(value));
        cv_.notify_one();
        co_return;
    }

    void push_sync(T value) {
        queue_.enqueue(std::move(value));
    }

    [[REMEMBER_CO_AWAIT]]
    auto pop() -> kosio::async::Task<T> {
        T value;
        if (queue_.try_dequeue(value)) {
            co_return value;
        }
        co_await mutex_.lock();
        std::unique_lock lock(mutex_, std::adopt_lock);
        while (!queue_.try_dequeue(value)) {
            co_await cv_.wait(mutex_, [this] { return queue_.size_approx() != 0; });
        }
        co_return value;
    }

private:
    moodycamel::ConcurrentQueue<T> queue_;
    kosio::sync::Mutex mutex_;
    kosio::sync::ConditionVariable cv_;
};
} // namespace foskv