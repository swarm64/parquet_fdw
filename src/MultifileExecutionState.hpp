#pragma once

#include "ParquetFdwExecutionState.hpp"
#include "ParquetFdwReader.hpp"

class MultifileExecutionState : public ParquetFdwExecutionState
{
private:
    struct FileRowgroups
    {
        std::string         filename;
        std::vector<int>    rowgroups;
    };
private:
    ParquetFdwReader       *reader;

    std::vector<FileRowgroups> files;
    uint64_t                cur_reader;

    MemoryContext           cxt;
    TupleDesc               tupleDesc;
    std::set<int>           attrs_used;
    bool                    use_threads;
    bool                    use_mmap;

    ParallelCoordinator    *coord;

private:
    ParquetFdwReader *get_next_reader()
    {
        ParquetFdwReader *r;

        if (coord)
            cur_reader = coord->next_reader.fetch_add(1, std::memory_order_relaxed);

        if (cur_reader >= files.size())
            return NULL;

        r = new ParquetFdwReader(cur_reader);
        r->open(files[cur_reader].filename.c_str(), cxt, tupleDesc, attrs_used, use_threads, use_mmap);
        r->set_rowgroups_list(files[cur_reader].rowgroups);

        cur_reader++;

        return r;
    }

public:
    MultifileExecutionState(MemoryContext cxt,
                            TupleDesc tupleDesc,
                            std::set<int> attrs_used,
                            bool use_threads,
                            bool use_mmap)
        : reader(NULL), cur_reader(0), cxt(cxt), tupleDesc(tupleDesc),
          attrs_used(attrs_used), use_threads(use_threads), use_mmap(use_mmap),
          coord(NULL)
    { }

    ~MultifileExecutionState()
    {
        if (reader)
            delete reader;
    }

    bool next(TupleTableSlot *slot, bool fake=false)
    {
        bool    res;

        if (unlikely(reader == NULL))
        {
            if ((reader = this->get_next_reader()) == NULL)
                return false;
        }

        res = reader->next(slot, fake);

        /* Finished reading current reader? Proceed to the next one */
        if (unlikely(!res))
        {
            while (true)
            {
                if (reader)
                    delete reader;

                reader = this->get_next_reader();
                if (!reader)
                    return false;
                res = reader->next(slot, fake);
                if (res)
                    break;
            }
        }

        if (res)
        {
            /*
             * ExecStoreVirtualTuple doesn't throw postgres exceptions thus no
             * need to wrap it into PG_TRY / PG_CATCH
             */
            ExecStoreVirtualTuple(slot);
        }

        return res;
    }

    void rescan(void)
    {
        reader->rescan();
    }

    void add_file(const char *filename, List *rowgroups)
    {
        FileRowgroups   fr;
        ListCell       *lc;

        fr.filename = filename;
        foreach (lc, rowgroups)
            fr.rowgroups.push_back(lfirst_int(lc));
        files.push_back(fr);
    }

    void set_coordinator(ParallelCoordinator *coord)
    {
        this->coord = coord;
    }
};
