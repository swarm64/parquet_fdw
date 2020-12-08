#pragma once

extern "C" {
#include "postgres.h"
#include "utils/memdebug.h"
}

#include "Misc.hpp"

class FastAllocator
{
private:
    /*
     * Special memory segment to speed up bytea/Text allocations.
     */
    MemoryContext       segments_cxt;
    char               *segment_start_ptr;
    char               *segment_cur_ptr;
    char               *segment_last_ptr;
    std::list<char *>   garbage_segments;

public:
    FastAllocator(MemoryContext cxt)
        : garbage_segments()
    {
        this->segments_cxt = cxt;
        this->segment_start_ptr = nullptr;
        this->segment_cur_ptr = nullptr;
        this->segment_last_ptr = nullptr;
    }

    /*
     * fast_alloc
     *      Preallocate a big memory segment and distribute blocks from it. When
     *      segment is exhausted it is added to garbage_segments list and freed
     *      on the next executor's iteration. If requested size is bigger that
     *      SEGMENT_SIZE then just palloc is used.
     */
    inline void *fast_alloc(long size)
    {
        void   *ret;

        Assert(size >= 0);

        /* If allocation is bigger than segment then just palloc */
        if (size > SEGMENT_SIZE)
            return palloc(size);

        size = MAXALIGN(size);

        /* If there is not enough space in current segment create a new one */
        if (this->segment_last_ptr - this->segment_cur_ptr < size)
        {
            MemoryContext oldcxt;

            /*
             * Recycle the last segment at the next iteration (if there
             * was one)
             */
            if (this->segment_start_ptr)
                this->garbage_segments.
                    push_back(this->segment_start_ptr);

            oldcxt = MemoryContextSwitchTo(this->segments_cxt);
            this->segment_start_ptr = (char *) exc_palloc(SEGMENT_SIZE);
            this->segment_cur_ptr = this->segment_start_ptr;
            this->segment_last_ptr =
                this->segment_start_ptr + SEGMENT_SIZE - 1;
            MemoryContextSwitchTo(oldcxt);
        }

        ret = (void *) this->segment_cur_ptr;
        this->segment_cur_ptr += size;

        return ret;
    }

    void recycle(void)
    {
        /* recycle old segments if any */
        if (!this->garbage_segments.empty())
        {
            bool error = false;

            PG_TRY();
            {
                for (auto it : this->garbage_segments)
                    pfree(it);
            }
            PG_CATCH();
            {
                error = true;
            }
            PG_END_TRY();
            if (error)
                elog(ERROR, "Garbage segments recycle failed");

            this->garbage_segments.clear();
            elog(DEBUG1, "parquet_fdw: garbage segments recycled");
        }
    }

    MemoryContext context()
    {
        return segments_cxt;
    }
};
