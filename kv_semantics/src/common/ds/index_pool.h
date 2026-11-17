/**
 * MIT License
 *
 * Copyright (c) 2025 Huawei Technologies Co., Ltd. All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * */
#ifndef UNIFIEDCACHE_INFRA_INDEX_POOL_H
#define UNIFIEDCACHE_INFRA_INDEX_POOL_H

#include <atomic>
#include <cstddef>
#include <limits>
#include <mutex>
#include <unordered_set>
#include <vector>

namespace kv {

class IndexPool {
public:
    using Index = uint32_t;
    static constexpr Index npos = std::numeric_limits<Index>::max();

private:
    struct Node {
        Index next;
    };
    struct Pointer {
        Index slot;
        uint32_t ver;
    };
    static_assert(sizeof(Pointer) == 8, "Pointer must be 64-bit");

public:
    void Setup(const Index capacity) noexcept
    {
        std::lock_guard<std::mutex> lock(quarantineMu_);
        this->capacity_ = capacity;
        this->nodes_.resize(capacity + 1);
        allocated_.reserve(capacity);
        quarantined_.reserve(capacity);
        for (Index slot = 1; slot <= capacity; ++slot) { this->nodes_[slot].next = slot + 1; }
        this->nodes_[capacity].next = 0;
        allocated_.clear();
        quarantined_.clear();
        this->pointer_.store({1, 0});
    }
    Index Acquire() noexcept
    {
        for (;;) {
            auto ptr = this->pointer_.load(std::memory_order_acquire);
            if (ptr.slot == 0) { return npos; }
            auto next = this->nodes_[ptr.slot].next;
            Pointer desired{next, ptr.ver + 1};
            if (this->pointer_.compare_exchange_weak(ptr, desired, std::memory_order_release,
                                                     std::memory_order_relaxed)) {
                const auto index = ptr.slot - 1;
                std::lock_guard<std::mutex> lock(quarantineMu_);
                allocated_.insert(index);
                return index;
            }
        }
    }
    void Release(const Index idx) noexcept
    {
        if (idx >= this->capacity_) { return; }
        std::lock_guard<std::mutex> lock(quarantineMu_);
        // A quarantined slot stays allocated and is not returned to the free list.
        if (quarantined_.find(idx) != quarantined_.end() ||
            allocated_.find(idx) == allocated_.end()) {
            return;
        }
        auto slot = idx + 1;
        for (;;) {
            auto ptr = this->pointer_.load(std::memory_order_acquire);
            this->nodes_[slot].next = ptr.slot;
            Pointer desired{slot, ptr.ver + 1};
            if (this->pointer_.compare_exchange_weak(ptr, desired, std::memory_order_release,
                                                     std::memory_order_relaxed)) {
                allocated_.erase(idx);
                return;
            }
        }
    }

    bool Quarantine(const Index idx) noexcept
    {
        if (idx >= this->capacity_) { return false; }
        std::lock_guard<std::mutex> lock(quarantineMu_);
        if (allocated_.find(idx) == allocated_.end()) { return false; }
        quarantined_.insert(idx);
        return true;
    }

    bool ReleaseQuarantined(const Index idx) noexcept
    {
        {
            std::lock_guard<std::mutex> lock(quarantineMu_);
            if (quarantined_.erase(idx) == 0) { return false; }
        }
        Release(idx);
        return true;
    }

    bool IsQuarantined(const Index idx) const noexcept
    {
        std::lock_guard<std::mutex> lock(quarantineMu_);
        return quarantined_.find(idx) != quarantined_.end();
    }

    bool IsAllocated(const Index idx) const noexcept
    {
        std::lock_guard<std::mutex> lock(quarantineMu_);
        return allocated_.find(idx) != allocated_.end();
    }

private:
    Index capacity_{0};
    std::vector<Node> nodes_;
    alignas(64) std::atomic<Pointer> pointer_;
    mutable std::mutex quarantineMu_;
    std::unordered_set<Index> allocated_;
    std::unordered_set<Index> quarantined_;
};

}  // namespace kv

#endif
