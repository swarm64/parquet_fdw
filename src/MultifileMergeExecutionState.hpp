#pragma once

#include "ParquetFdwExecutionState.hpp"
#include "ParquetFdwReader.hpp"

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
    bool compare_slots(FileSlot &a, FileSlot &b)
    {
        bool    error = false;

        PG_TRY();
        {
            TupleTableSlot *s1 = a.slot;
            TupleTableSlot *s2 = b.slot;

            Assert(!TupIsNull(s1));
            Assert(!TupIsNull(s2));

            for (auto sort_key: sort_keys)
            {
                AttrNumber  attno = sort_key.ssup_attno;
                Datum       datum1,
                            datum2;
                bool        isNull1,
                            isNull2;
                int         compare;

                datum1 = slot_getattr(s1, attno, &isNull1);
                datum2 = slot_getattr(s2, attno, &isNull2);

                compare = ApplySortComparator(datum1, isNull1,
                                              datum2, isNull2,
                                              &sort_key);
                if (compare != 0)
                    return (compare > 0);
            }
        }
        PG_CATCH();
        {
            error = true;
        }
        PG_END_TRY();

        if (error)
            throw std::runtime_error("slots comparison failed");

        return false;
    }

public:
    MultifileMergeExecutionState(MemoryContext cxt,
                                 TupleDesc tupleDesc,
                                 std::set<int> attrs_used,
                                 std::list<SortSupportData> sort_keys,
                                 bool use_threads,
                                 bool use_mmap)
        : cxt(cxt), tupleDesc(tupleDesc), attrs_used(attrs_used),
          sort_keys(sort_keys), use_threads(use_threads), use_mmap(use_mmap),
          slots_initialized(false)
    { }

    ~MultifileMergeExecutionState()
    {
#if PG_VERSION_NUM < 110000
        /* Destroy tuple slots if any */
        for (auto it: slots)
            ExecDropSingleTupleTableSlot(it.slot);
#endif

        for (auto it: readers)
            delete it;
    }

    bool next(TupleTableSlot *slot, bool fake=false)
    {
        bool error = false;
        auto cmp = [this] (FileSlot &a, FileSlot &b) { return compare_slots(a, b); };

        if (unlikely(!slots_initialized))
        {
            /* Initialize binary heap on the first run */
            int i = 0;

            for (auto reader: readers)
            {
                FileSlot    fs;
                bool        error = false;

                PG_TRY();
                {
                    MemoryContext oldcxt;

                    oldcxt = MemoryContextSwitchTo(cxt);
#if PG_VERSION_NUM < 110000
                    fs.slot = MakeSingleTupleTableSlot(tupleDesc);
#elif PG_VERSION_NUM < 120000
                    fs.slot = MakeTupleTableSlot(tupleDesc);
#else
                    fs.slot = MakeTupleTableSlot(tupleDesc, &TTSOpsVirtual);
#endif
                    MemoryContextSwitchTo(oldcxt);
                }
                PG_CATCH();
                {
                    error = true;
                }
                PG_END_TRY();

                if (error)
                    throw std::runtime_error("failed to create a TupleTableSlot");

                if (reader->next(fs.slot))
                {
                    ExecStoreVirtualTuple(fs.slot);
                    fs.reader_id = i;
                    slots.push_back(fs);
                }
                ++i;
            }
            std::make_heap(slots.begin(), slots.end(), cmp);
            slots_initialized = true;
        }

        if (unlikely(slots.empty()))
            return false;

        const FileSlot &fs = slots.front();

        PG_TRY();
        {
            ExecCopySlot(slot, fs.slot);
            ExecClearTuple(fs.slot);
        }
        PG_CATCH();
        {
            error = true;
        }
        PG_END_TRY();
        if (error)
            throw std::runtime_error("failed to copy a virtual tuple slot");

        if (readers[fs.reader_id]->next(fs.slot))
        {
            ExecStoreVirtualTuple(fs.slot);
        }
        else
        {
            /* Finished reading from this reader */
#if PG_VERSION_NUM < 110000
            PG_TRY();
            {
                ExecDropSingleTupleTableSlot(fs.slot);
            }
            PG_CATCH();
            {
                error = true;
            }
            PG_END_TRY();
            if (error)
                throw std::runtime_error("failed to drop a tuple slot");
#endif
            std::pop_heap(slots.begin(), slots.end(), cmp);
            slots.pop_back();
        }
        std::make_heap(slots.begin(), slots.end(), cmp);
        return true;
    }

    void rescan(void)
    {
        /* TODO: clean binheap */
        for (auto reader: readers)
            reader->rescan();
        slots.clear();
        slots_initialized = false;
    }

    void add_file(const char *filename, List *rowgroups)
    {
        ParquetFdwReader *r;
        ListCell         *lc;
        std::vector<int>    rg;

        foreach (lc, rowgroups)
            rg.push_back(lfirst_int(lc));

        r = new ParquetFdwReader(0);
        r->open(filename, cxt, tupleDesc, attrs_used, use_threads, use_mmap);
        r->set_rowgroups_list(rg);
        readers.push_back(r);
    }

    void set_coordinator(ParallelCoordinator *coord)
    {
        Assert(true);   /* not supported, should never happen */
    }
};
