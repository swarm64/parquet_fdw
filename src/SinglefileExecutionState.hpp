#pragma once


#include "ParquetFdwExecutionState.hpp"
#include "ParquetFdwReader.hpp"


class SingleFileExecutionState : public ParquetFdwExecutionState
{
private:
    ParquetFdwReader   *reader;
    MemoryContext       cxt;
    TupleDesc           tupleDesc;
    std::set<int>       attrs_used;
    bool                use_mmap;
    bool                use_threads;

public:
    MemoryContext       estate_cxt;

    SingleFileExecutionState(MemoryContext cxt,
                             TupleDesc tupleDesc,
                             std::set<int> attrs_used,
                             bool use_threads,
                             bool use_mmap)
    ;

    ~SingleFileExecutionState();

    bool next(TupleTableSlot *slot, bool fake);
    void rescan(void);
    void add_file(const char *filename, List *rowgroups);
    void set_coordinator(ParallelCoordinator *coord);
};
