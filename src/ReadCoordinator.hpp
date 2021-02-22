
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <vector>

class ParquetFdwReader;

class ReadCoordinator
{
private:
    std::atomic_uint64_t currentReadListIndex;

public:
    ReadCoordinator()
    {
        reset();
    }

    void reset()
    {
        currentReadListIndex.store(0);
    }

    const uint64_t getNextReadListItem()
    {
        return currentReadListIndex.fetch_add(1, std::memory_order_relaxed);
    }
};
