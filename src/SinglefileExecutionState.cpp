
#include "SinglefileExecutionState.hpp"

extern "C" {
#include "miscadmin.h"
}

SingleFileExecutionState::SingleFileExecutionState(MemoryContext cxt,
                         TupleDesc tupleDesc,
                         std::set<int> attrs_used,
                         bool use_threads,
                         bool use_mmap)
    : cxt(cxt), tupleDesc(tupleDesc), attrs_used(attrs_used),
      use_mmap(use_mmap), use_threads(use_threads)
{ }

SingleFileExecutionState::~SingleFileExecutionState()
{
    if (reader)
        delete reader;
}

bool SingleFileExecutionState::next(TupleTableSlot *slot, bool fake)
{
    bool res;

    if ((res = reader->next(slot, fake)) == true)
        ExecStoreVirtualTuple(slot);

    return res;
}

void SingleFileExecutionState::rescan(void)
{
    reader->rescan();
}

void SingleFileExecutionState::add_file(const char *filename, List *rowgroups)
{
    ListCell           *lc;
    std::vector<int>    rg;
    // int                 reader_id = 0;

    foreach (lc, rowgroups)
        rg.push_back(lfirst_int(lc));

    reader = new ParquetFdwReader(MyProcPid);
    reader->open(filename, cxt, tupleDesc, attrs_used, use_threads, use_mmap);
    reader->set_rowgroups_list(rg);
}

void SingleFileExecutionState::set_coordinator(ParallelCoordinator *coord)
{
    if (reader)
        reader->coordinator = coord;
}
