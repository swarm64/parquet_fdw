
#pragma once

#include <atomic>
#include <cstdint>

struct ParallelCoordinator
{
    std::atomic<int32_t> next_reader;
    std::atomic<int32_t> next_rowgroup;
};
