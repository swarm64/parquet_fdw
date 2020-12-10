#pragma once

#include "ParquetFdwReader.hpp"
#include "ReadCoordinator.hpp"

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
    struct FileRowgroups
    {
        std::string      filename;
        std::vector<int> rowgroups;
    };

    std::shared_ptr<ParquetFdwReader> currentReader;

protected:
    // ParquetFdwReader       *reader;

    std::vector<FileRowgroups>                     files;
    std::vector<std::shared_ptr<ParquetFdwReader>> readers;

    // uint64_t                cur_reader;

    MemoryContext cxt;
    TupleDesc     tupleDesc;
    // std::set<int> attrs_used;
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
                             // std::set<int> attrs_used,
                             bool use_threads,
                             bool use_mmap);

    ~ParquetFdwExecutionState();

    bool next(TupleTableSlot *slot, bool fake = false);
    void add_file(const char *filename, List *rowgroups);
    void set_coordinator(ReadCoordinator *coord);
    void fillReadList();

    const std::vector<int> &getRowGroupsForFile(const int fileId) const
    {
        return files[fileId].rowgroups;
    }

    void rescan()
    {
        if (!coord)
            Error("Coordinator not set");
        coord->reset();
    }
};
