
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <vector>

class ParquetFdwReader;

struct ParallelCoordinator {
    std::atomic<int32_t> next_reader;
    std::atomic<int32_t> next_rowgroup;
};
