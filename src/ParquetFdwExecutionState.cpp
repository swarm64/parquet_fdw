#include <sstream>
#include <utility>

#include "ParquetFdwExecutionState.hpp"
#include "ReadCoordinator.hpp"

extern "C" {
#include "miscadmin.h"
}

ParquetFdwExecutionState::ParquetFdwExecutionState(MemoryContext            cxt,
                                                   TupleDesc                tupleDesc,
                                                   const std::vector<bool> &attrUseList,
                                                   // std::set<int> attrs_used,
                                                   bool use_threads,
                                                   bool use_mmap)
    : cxt(cxt),
      tupleDesc(tupleDesc),
      attrUseList(attrUseList),
      // attrs_used(std::move(attrs_used)),
      use_threads(use_threads),
      use_mmap(use_mmap),
      coord(new ReadCoordinator())
{
}

ParquetFdwExecutionState::~ParquetFdwExecutionState()
{
    readers.clear();
}

bool ParquetFdwExecutionState::next(TupleTableSlot *slot, bool fake)
{
    if (unlikely(coord == nullptr))
        throw std::runtime_error("Coordinator not set");

    if (!currentReader || currentReader->finishedReadingRowGroup())
    {
        const uint64_t nextReadListItem = coord->getNextReadListItem();
        if (nextReadListItem >= readList.size())
            return false;

        const auto [readerId, rowGroupId] = readList[nextReadListItem];

        if (readerId >= (int)readers.size())
        {
            std::stringstream ss;
            ss << MyProcPid << " reader id " << readerId << " out of range";
            throw std::runtime_error(ss.str());
        }

        currentReader = readers[readerId];
        // throw Error("Worker %d switches to %d %d", MyProcPid, readerId, rowGroupId);
        currentReader->prepareToReadRowGroup(rowGroupId, tupleDesc);
    }

    const bool res = currentReader->next(slot, fake);
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

void ParquetFdwExecutionState::rescan()
{
    throw std::runtime_error("rescan not implemented...");
    // reader->rescan();
}

void ParquetFdwExecutionState::add_file(const char *filename, List *rowgroups)
{
    FileRowgroups fr;
    ListCell *    lc;

    fr.filename = filename;
    if (rowgroups == nullptr)
        fr.rowgroups.push_back(0);
    else
    {
        foreach (lc, rowgroups)
            fr.rowgroups.push_back(lfirst_int(lc));
    }

    files.push_back(fr);
    const auto reader = std::make_shared<ParquetFdwReader>(filename);
    reader->open(filename, cxt, tupleDesc, attrUseList, use_threads, use_mmap);
    reader->set_rowgroups_list(fr.rowgroups);
    readers.push_back(reader);
}

void ParquetFdwExecutionState::set_coordinator(ReadCoordinator *coord)
{
    this->coord = coord;
}

void ParquetFdwExecutionState::fillReadList()
{
    for (int readerId = 0; readerId < (int)files.size(); readerId++)
    {
        for (const int rowGroupId : files[readerId].rowgroups)
        {
            readList.push_back({readerId, rowGroupId});
        }
    }
}
