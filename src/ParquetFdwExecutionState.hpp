
#pragma once

struct TupleTableSlot;
struct List;

class ParquetFdwExecutionState
{
public:
    virtual ~ParquetFdwExecutionState() {};
    virtual bool next(TupleTableSlot *slot, bool fake=false) = 0;
    virtual void rescan(void) = 0;
    virtual void add_file(const char *filename, List *rowgroups) = 0;
    virtual void set_coordinator(ParallelCoordinator *coord) = 0;

    int stateId;
};
