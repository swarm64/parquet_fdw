#pragma once

#include "ParquetFdwReader.hpp"
#include "ReadCoordinator.hpp"
#include "utils/palloc.h"

#include <set>
#include <string>
#include <vector>

extern "C" {
#include "access/tupdesc.h"
#include "postgres.h"
}

class ParquetFdwExecutionState
{
private:
    std::shared_ptr<ParquetFdwReader> currentReader;

protected:
    std::vector<std::shared_ptr<ParquetFdwReader>> readers;

    MemoryContext cxt;
    TupleDesc     tupleDesc;
    std::vector<bool> attrUseList;
    bool              use_threads;
    bool              use_mmap;

    ReadCoordinator *coord;

private:
    using tReadListEntry = std::tuple<int32_t, int32_t>;
    using tReadList      = std::vector<tReadListEntry>;

    tReadList readList;

public:
    ParquetFdwExecutionState(MemoryContext            cxt,
                             TupleDesc                tupleDesc,
                             const std::vector<bool> &attrUseList,
                             bool use_threads,
                             bool use_mmap);

    ~ParquetFdwExecutionState();

    bool next(TupleTableSlot *slot, bool fake = false);
    void set_coordinator(ReadCoordinator *coord);
    void addFileToRead(const char* path, MemoryContext cxt, const List* rowGroupSkipList);

    void rescan()
    {
        if (!coord)
            Error("Coordinator not set");
        coord->reset();
    }
};
