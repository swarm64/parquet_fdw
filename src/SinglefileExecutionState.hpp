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
        : cxt(cxt), tupleDesc(tupleDesc), attrs_used(attrs_used),
          use_mmap(use_mmap), use_threads(use_threads)
    { }

    ~SingleFileExecutionState()
    {
        if (reader)
            delete reader;
    }

    bool next(TupleTableSlot *slot, bool fake)
    {
        bool res;

        if ((res = reader->next(slot, fake)) == true)
            ExecStoreVirtualTuple(slot);

        return res;
    }

    void rescan(void)
    {
        reader->rescan();
    }

    void add_file(const char *filename, List *rowgroups)
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

    void set_coordinator(ParallelCoordinator *coord)
    {
        if (reader)
            reader->coordinator = coord;
    }
};
