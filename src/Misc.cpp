
#include "Misc.h"

/*
 * exc_palloc
 *      C++ specific memory allocator that utilizes postgres allocation sets.
 */
void *
exc_palloc(Size size)
{
	/* duplicates MemoryContextAllocZero to avoid increased overhead */
	void	   *ret;
	MemoryContext context = CurrentMemoryContext;

	AssertArg(MemoryContextIsValid(context));

	if (!AllocSizeIsValid(size))
		throw std::bad_alloc();

	context->isReset = false;

	ret = context->methods->alloc(context, size);
	if (unlikely(ret == NULL))
		throw std::bad_alloc();

	VALGRIND_MEMPOOL_ALLOC(context, ret, size);

	return ret;
}
