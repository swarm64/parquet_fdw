#include <sstream>
#include <utility>

#include "ParquetFdwExecutionState.hpp"
#include "ReadCoordinator.hpp"
#include "utils/palloc.h"

extern "C" {
#include "miscadmin.h"
}

ParquetFdwExecutionState::ParquetFdwExecutionState(MemoryContext            cxt,
                                                   TupleDesc                tupleDesc,
                                                   const std::vector<bool> &attrUseList,
                                                   bool                     use_mmap)
    : cxt(cxt),
      tupleDesc(tupleDesc),
      attrUseList(attrUseList),
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
        currentReader->bufferRowGroup(rowGroupId, tupleDesc, attrUseList);
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

void ParquetFdwExecutionState::addFileToRead(const char* path, MemoryContext cxt, const List* rowGroupSkipList) {
    std::set<int> rowGroupsToSkip;
    if (rowGroupSkipList) {
        ListCell *lc;
        foreach(lc, rowGroupSkipList) {
            rowGroupsToSkip.insert(lfirst_int(lc));
        }
    }

    const auto sharedReader = std::make_shared<ParquetFdwReader>(path);
    sharedReader->setMemoryContext(cxt);
    readers.push_back(sharedReader);

    const auto readerId = readers.size() - 1;
    const auto numRowGroups = (int32_t)(sharedReader->getNumRowGroups());

    for (int rowGroupId = 0; rowGroupId < numRowGroups; ++rowGroupId) {
        if (rowGroupsToSkip.find(rowGroupId) != rowGroupsToSkip.cend())
            continue;

        readList.push_back({readerId, rowGroupId});
    }
}

void ParquetFdwExecutionState::set_coordinator(ReadCoordinator *coord)
{
    this->coord = coord;
}

