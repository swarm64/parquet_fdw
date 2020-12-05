#pragma once

#include "ParquetFdwExecutionState.hpp"
#include "ParquetFdwReader.hpp"

#include <set>
#include <string>
#include <vector>

extern "C" {
#include "postgres.h"
#include "access/tupdesc.h"
}

class MultifileExecutionState : public ParquetFdwExecutionState
{
private:
    struct FileRowgroups {
        std::string         filename;
        std::vector<int>    rowgroups;
    };

private:
    // ParquetFdwReader       *reader;

    std::vector<FileRowgroups> files;
    std::vector<std::shared_ptr<ParquetFdwReader>> readers;

    // uint64_t                cur_reader;

    MemoryContext           cxt;
    TupleDesc               tupleDesc;
    std::set<int>           attrs_used;
    bool                    use_threads;
    bool                    use_mmap;

    ParallelCoordinator    *coord;

private:
    // bool messageDone = false;
    // ParquetFdwReader *get_next_reader();

public:
    MultifileExecutionState(MemoryContext cxt,
                            TupleDesc tupleDesc,
                            std::set<int> attrs_used,
                            bool use_threads,
                            bool use_mmap)
    ;

    ~MultifileExecutionState();

    bool next(TupleTableSlot *slot, bool fake=false);
    void rescan(void);
    void add_file(const char *filename, List *rowgroups);
    void set_coordinator(ParallelCoordinator *coord);
};
