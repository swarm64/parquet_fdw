#pragma once

#include "ParquetFdwExecutionState.hpp"
#include "ParquetFdwReader.hpp"

extern "C" {
#include "utils/sortsupport.h"
}

class MultifileMergeExecutionState : public ParquetFdwExecutionState
{
    struct FileSlot
    {
        int             reader_id;
        TupleTableSlot *slot;
    };
    typedef std::vector<FileSlot> BinHeap;
private:
    std::vector<ParquetFdwReader *>   readers;

    MemoryContext       cxt;
    TupleDesc           tupleDesc;
    std::set<int>       attrs_used;
    std::list<SortSupportData> sort_keys;
    bool                use_threads;
    bool                use_mmap;

    /*
     * Heap is used to store tuples in prioritized manner along with file
     * number. Priority is given to the tuples with minimal key. Once next
     * tuple is requested it is being taken from the top of the heap and a new
     * tuple from the same file is read and inserted back into the heap. Then
     * heap is rebuilt to sustain its properties. The idea is taken from
     * nodeGatherMerge.c in PostgreSQL but reimplemented using STL.
     */
    BinHeap             slots;
    bool                slots_initialized;

private:
    /*
     * Compares two slots according to sort keys. Returns true if a > b,
     * false otherwise. The function is stolen from nodeGatherMerge.c
     * (postgres) and adapted.
     */
    bool compare_slots(FileSlot &a, FileSlot &b);

public:
    MultifileMergeExecutionState(MemoryContext cxt,
                                 TupleDesc tupleDesc,
                                 std::set<int> attrs_used,
                                 std::list<SortSupportData> sort_keys,
                                 bool use_threads,
                                 bool use_mmap);

    ~MultifileMergeExecutionState();

    bool next(TupleTableSlot *slot, bool fake=false);
    void rescan(void);
    void add_file(const char *filename, List *rowgroups);
    void set_coordinator(ParallelCoordinator *coord);
};
