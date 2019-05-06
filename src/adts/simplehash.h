/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
/*
 * This file contains source code that was copied and/or modified from
 * the PostgreSQL database, which is licensed under the open-source
 * PostgreSQL License. Please see the NOTICE at the top level
 * directory for a copy of the PostgreSQL License.
 *
 * Port of postgres simplehash.h
 *
 * simplehash.h
 *
 *	  Hash table implementation which will be specialized to user-defined
 *	  types, by including this file to generate the required code.  It's
 *	  probably not worthwhile to do so for hash tables that aren't performance
 *	  or space sensitive.
 *
 * Usage notes:
 *
 *	  To generate a hash-table and associated functions for a use case several
 *	  macros have to be #define'ed before this file is included.  Including
 *	  the file #undef's all those, so a new hash table can be generated
 *	  afterwards.
 *	  The relevant parameters are:
 *	  - SH_PREFIX - prefix for all symbol names generated. A prefix of 'foo'
 *		will result in hash table type 'foo_hash' and functions like
 *		'foo_insert'/'foo_lookup' and so forth.
 *	  - SH_ELEMENT_TYPE - type of the contained elements
 *	  - SH_KEY_TYPE - type of the hashtable's key
 *	  - SH_DECLARE - if defined function prototypes and type declarations are
 *		generated
 *	  - SH_DEFINE - if defined function definitions are generated
 *	  - SH_SCOPE - in which scope (e.g. extern, static inline) do function
 *		declarations reside
 *	  - SH_USE_NONDEFAULT_ALLOCATOR - if defined no element allocator functions
 *		are defined, so you can supply your own
 *	  The following parameters are only relevant when SH_DEFINE is defined:
 *	  - SH_KEY - name of the element in SH_ELEMENT_TYPE containing the hash key
 *	  - SH_EQUAL(table, a, b) - compare two table keys
 *	  - SH_HASH_KEY(table, key) - generate hash for the key
 *	  - SH_STORE_HASH - if defined the hash is stored in the elements
 *	  - SH_GET_HASH(tb, a) - return the field to store the hash in
 *
 *	  For examples of usage look at tidbitmap.c (file local definition) and
 *	  execnodes.h/execGrouping.c (exposed declaration, file local
 *	  implementation).
 *
 * Hash table design:
 *
 *	  The hash table design chosen is a variant of linear open-addressing. The
 *	  reason for doing so is that linear addressing is CPU cache & pipeline
 *	  friendly. The biggest disadvantage of simple linear addressing schemes
 *	  are highly variable lookup times due to clustering, and deletions
 *	  leaving a lot of tombstones around.  To address these issues a variant
 *	  of "robin hood" hashing is employed.  Robin hood hashing optimizes
 *	  chaining lengths by moving elements close to their optimal bucket
 *	  ("rich" elements), out of the way if a to-be-inserted element is further
 *	  away from its optimal position (i.e. it's "poor").  While that can make
 *	  insertions slower, the average lookup performance is a lot better, and
 *	  higher fill factors can be used in a still performant manner.  To avoid
 *	  tombstones - which normally solve the issue that a deleted node's
 *	  presence is relevant to determine whether a lookup needs to continue
 *	  looking or is done - buckets following a deleted element are shifted
 *	  backwards, unless they're empty or already at their optimal position.
 */

/* helpers */
#define SH_MAKE_PREFIX(a) CppConcat(a, _)
#define SH_MAKE_NAME(name) SH_MAKE_NAME_(SH_MAKE_PREFIX(SH_PREFIX), name)
#define SH_MAKE_NAME_(a, b) CppConcat(a, b)

/* name macros for: */

/* type declarations */
#define SH_TYPE SH_MAKE_NAME(hash)
#define SH_STATUS SH_MAKE_NAME(status)
#define SH_STATUS_EMPTY SH_MAKE_NAME(SH_EMPTY)
#define SH_STATUS_IN_USE SH_MAKE_NAME(SH_IN_USE)
#define SH_ITERATOR SH_MAKE_NAME(iterator)

/* function declarations */
#define SH_CREATE SH_MAKE_NAME(create)
#define SH_DESTROY SH_MAKE_NAME(destroy)
#define SH_RESET SH_MAKE_NAME(reset)
#define SH_INSERT SH_MAKE_NAME(insert)
#define SH_DELETE SH_MAKE_NAME(delete)
#define SH_LOOKUP SH_MAKE_NAME(lookup)
#define SH_GROW SH_MAKE_NAME(grow)
#define SH_START_ITERATE SH_MAKE_NAME(start_iterate)
#define SH_START_ITERATE_AT SH_MAKE_NAME(start_iterate_at)
#define SH_ITERATE SH_MAKE_NAME(iterate)
#define SH_ALLOCATE SH_MAKE_NAME(allocate)
#define SH_FREE SH_MAKE_NAME(free)
#define SH_STAT SH_MAKE_NAME(stat)

/* internal helper functions (no externally visible prototypes) */
#define SH_COMPUTE_PARAMETERS SH_MAKE_NAME(compute_parameters)
#define SH_NEXT SH_MAKE_NAME(next)
#define SH_PREV SH_MAKE_NAME(prev)
#define SH_DISTANCE_FROM_OPTIMAL SH_MAKE_NAME(distance)
#define SH_INITIAL_BUCKET SH_MAKE_NAME(initial_bucket)
#define SH_ENTRY_HASH SH_MAKE_NAME(entry_hash)

/* generate forward declarations necessary to use the hash table */
#ifdef SH_DECLARE

/* type definitions */
typedef struct SH_TYPE
{
	/*
	 * Size of data / bucket array, 64 bits to handle UINT32_MAX sized hash
	 * tables.  Note that the maximum number of elements is lower
	 * (SH_MAX_FILLFACTOR)
	 */
	uint64 size;

	/* how many elements have valid contents */
	uint32 members;

	/* mask for bucket and size calculations, based on size */
	uint32 sizemask;

	/* boundary after which to grow hashtable */
	uint32 grow_threshold;

	/* hash buckets */
	SH_ELEMENT_TYPE *data;

	/* memory context to use for allocations */
	MemoryContext ctx;

	/* user defined data, useful for callbacks */
	void *private_data;
} SH_TYPE;

typedef enum SH_STATUS
{
	SH_STATUS_EMPTY = 0x00,
	SH_STATUS_IN_USE = 0x01
} SH_STATUS;

typedef struct SH_ITERATOR
{
	uint32 cur; /* current element */
	uint32 end;
	bool done; /* iterator exhausted? */
} SH_ITERATOR;

/* externally visible function prototypes */
SH_SCOPE SH_TYPE *SH_CREATE(MemoryContext ctx, uint32 nelements, void *private_data);
SH_SCOPE void SH_DESTROY(SH_TYPE *tb);
SH_SCOPE void SH_RESET(SH_TYPE *tb);
SH_SCOPE void SH_GROW(SH_TYPE *tb, uint32 newsize);
SH_SCOPE SH_ELEMENT_TYPE *SH_INSERT(SH_TYPE *tb, SH_KEY_TYPE key, bool *found);
SH_SCOPE SH_ELEMENT_TYPE *SH_LOOKUP(SH_TYPE *tb, SH_KEY_TYPE key);
SH_SCOPE bool SH_DELETE(SH_TYPE *tb, SH_KEY_TYPE key);
SH_SCOPE void SH_START_ITERATE(SH_TYPE *tb, SH_ITERATOR *iter);
SH_SCOPE void SH_START_ITERATE_AT(SH_TYPE *tb, SH_ITERATOR *iter, uint32 at);
SH_SCOPE SH_ELEMENT_TYPE *SH_ITERATE(SH_TYPE *tb, SH_ITERATOR *iter);
SH_SCOPE void SH_STAT(SH_TYPE *tb);

#endif /* SH_DECLARE */

/* generate implementation of the hash table */
#ifdef SH_DEFINE

#include "utils/memutils.h"

/* max data array size,we allow up to PG_UINT32_MAX buckets, including 0 */
#define SH_MAX_SIZE (((uint64) PG_UINT32_MAX) + 1)

/* normal fillfactor, unless already close to maximum */
#ifndef SH_FILLFACTOR
#define SH_FILLFACTOR (0.9)
#endif
/* increase fillfactor if we otherwise would error out */
#define SH_MAX_FILLFACTOR (0.98)
/* grow if actual and optimal location bigger than */
#ifndef SH_GROW_MAX_DIB
#define SH_GROW_MAX_DIB 25
#endif
/* grow if more than elements to move when inserting */
#ifndef SH_GROW_MAX_MOVE
#define SH_GROW_MAX_MOVE 150
#endif
#ifndef SH_GROW_MIN_FILLFACTOR
/* but do not grow due to SH_GROW_MAX_* if below */
#define SH_GROW_MIN_FILLFACTOR 0.1
#endif

#ifdef SH_STORE_HASH
#define SH_COMPARE_KEYS(tb, ahash, akey, b)                                                        \
	(ahash == SH_GET_HASH(tb, b) && SH_EQUAL(tb, b->SH_KEY, akey))
#else
#define SH_COMPARE_KEYS(tb, ahash, akey, b) (SH_EQUAL(tb, b->SH_KEY, akey))
#endif

/*
 * Wrap the following definitions in include guards, to avoid multiple
 * definition errors if this header is included more than once.  The rest of
 * the file deliberately has no include guards, because it can be included
 * with different parameters to define functions and types with non-colliding
 * names.
 */
#ifndef SIMPLEHASH_H
#define SIMPLEHASH_H

/* FIXME: can we move these to a central location? */

/* calculate ceil(log base 2) of num */
static inline uint64
sh_log2(uint64 num)
{
	int i;
	uint64 limit;

	for (i = 0, limit = 1; limit < num; i++, limit <<= 1)
		;
	return i;
}

/* calculate first power of 2 >= num */
static inline uint64
sh_pow2(uint64 num)
{
	return ((uint64) 1) << sh_log2(num);
}

#endif

/*
 * Compute sizing parameters for hashtable. Called when creating and growing
 * the hashtable.
 */
static inline void
SH_COMPUTE_PARAMETERS(SH_TYPE *tb, uint32 newsize)
{
	uint64 size;

	/* supporting zero sized hashes would complicate matters */
	size = Max(newsize, 2);

	/* round up size to the next power of 2, that's how bucketing works */
	size = sh_pow2(size);
	Assert(size <= SH_MAX_SIZE);

	/*
	 * Verify that allocation of ->data is possible on this platform, without
	 * overflowing Size.
	 */
	if ((((uint64) sizeof(SH_ELEMENT_TYPE)) * size) >= MaxAllocHugeSize)
		elog(ERROR, "hash table too large");

	/* now set size */
	tb->size = size;

	if (tb->size == SH_MAX_SIZE)
		tb->sizemask = 0;
	else
		tb->sizemask = tb->size - 1;

	/*
	 * Compute the next threshold at which we need to grow the hash table
	 * again.
	 */
	if (tb->size == SH_MAX_SIZE)
		tb->grow_threshold = ((double) tb->size) * SH_MAX_FILLFACTOR;
	else
		tb->grow_threshold = ((double) tb->size) * SH_FILLFACTOR;
}

/* return the optimal bucket for the hash */
static inline uint32
SH_INITIAL_BUCKET(SH_TYPE *tb, uint32 hash)
{
	return hash & tb->sizemask;
}

/* return next bucket after the current, handling wraparound */
static inline uint32
SH_NEXT(SH_TYPE *tb, uint32 curelem, uint32 startelem)
{
	curelem = (curelem + 1) & tb->sizemask;

	Assert(curelem != startelem);

	return curelem;
}

/* return bucket before the current, handling wraparound */
static inline uint32
SH_PREV(SH_TYPE *tb, uint32 curelem, uint32 startelem)
{
	curelem = (curelem - 1) & tb->sizemask;

	Assert(curelem != startelem);

	return curelem;
}

/* return distance between bucket and its optimal position */
static inline uint32
SH_DISTANCE_FROM_OPTIMAL(SH_TYPE *tb, uint32 optimal, uint32 bucket)
{
	if (optimal <= bucket)
		return bucket - optimal;
	else
		return (tb->size + bucket) - optimal;
}

static inline uint32
SH_ENTRY_HASH(SH_TYPE *tb, SH_ELEMENT_TYPE *entry)
{
#ifdef SH_STORE_HASH
	return SH_GET_HASH(tb, entry);
#else
	return SH_HASH_KEY(tb, entry->SH_KEY);
#endif
}

/* default memory allocator function */
static inline void *SH_ALLOCATE(SH_TYPE *type, Size size);
static inline void SH_FREE(SH_TYPE *type, void *pointer);

#ifndef SH_USE_NONDEFAULT_ALLOCATOR

/* default memory allocator function */
static inline void *
SH_ALLOCATE(SH_TYPE *type, Size size)
{
	return MemoryContextAllocExtended(type->ctx, size, MCXT_ALLOC_HUGE | MCXT_ALLOC_ZERO);
}

/* default memory free function */
static inline void
SH_FREE(SH_TYPE *type, void *pointer)
{
	pfree(pointer);
}

#endif

/*
 * Create a hash table with enough space for `nelements` distinct members.
 * Memory for the hash table is allocated from the passed-in context.  If
 * desired, the array of elements can be allocated using a passed-in allocator;
 * this could be useful in order to place the array of elements in a shared
 * memory, or in a context that will outlive the rest of the hash table.
 * Memory other than for the array of elements will still be allocated from
 * the passed-in context.
 */
SH_SCOPE SH_TYPE *
SH_CREATE(MemoryContext ctx, uint32 nelements, void *private_data)
{
	SH_TYPE *tb;
	uint64 size;

	tb = MemoryContextAllocZero(ctx, sizeof(SH_TYPE));
	tb->ctx = ctx;
	tb->private_data = private_data;

	/* increase nelements by fillfactor, want to store nelements elements */
	size = Min((double) SH_MAX_SIZE, ((double) nelements) / SH_FILLFACTOR);

	SH_COMPUTE_PARAMETERS(tb, size);

	tb->data = SH_ALLOCATE(tb, sizeof(SH_ELEMENT_TYPE) * tb->size);

	return tb;
}

/* destroy a previously created hash table */
SH_SCOPE void
SH_DESTROY(SH_TYPE *tb)
{
	SH_FREE(tb, tb->data);
	pfree(tb);
}

/* reset the contents of a previously created hash table */
SH_SCOPE void
SH_RESET(SH_TYPE *tb)
{
	memset(tb->data, 0, sizeof(SH_ELEMENT_TYPE) * tb->size);
	tb->members = 0;
}

/*
 * Grow a hash table to at least `newsize` buckets.
 *
 * Usually this will automatically be called by insertions/deletions, when
 * necessary. But resizing to the exact input size can be advantageous
 * performance-wise, when known at some point.
 */
SH_SCOPE void
SH_GROW(SH_TYPE *tb, uint32 newsize)
{
	uint64 oldsize = tb->size;
	SH_ELEMENT_TYPE *olddata = tb->data;
	SH_ELEMENT_TYPE *newdata;
	uint32 i;
	uint32 startelem = 0;
	uint32 copyelem;

	Assert(oldsize == sh_pow2(oldsize));
	Assert(oldsize != SH_MAX_SIZE);
	Assert(oldsize < newsize);

	/* compute parameters for new table */
	SH_COMPUTE_PARAMETERS(tb, newsize);

	tb->data = SH_ALLOCATE(tb, sizeof(SH_ELEMENT_TYPE) * tb->size);

	newdata = tb->data;

	/*
	 * Copy entries from the old data to newdata. We theoretically could use
	 * SH_INSERT here, to avoid code duplication, but that's more general than
	 * we need. We neither want tb->members increased, nor do we need to do
	 * deal with deleted elements, nor do we need to compare keys. So a
	 * special-cased implementation is lot faster. As resizing can be time
	 * consuming and frequent, that's worthwhile to optimize.
	 *
	 * To be able to simply move entries over, we have to start not at the
	 * first bucket (i.e olddata[0]), but find the first bucket that's either
	 * empty, or is occupied by an entry at its optimal position. Such a
	 * bucket has to exist in any table with a load factor under 1, as not all
	 * buckets are occupied, i.e. there always has to be an empty bucket.  By
	 * starting at such a bucket we can move the entries to the larger table,
	 * without having to deal with conflicts.
	 */

	/* search for the first element in the hash that's not wrapped around */
	for (i = 0; i < oldsize; i++)
	{
		SH_ELEMENT_TYPE *oldentry = &olddata[i];
		uint32 hash;
		uint32 optimal;

		if (oldentry->status != SH_STATUS_IN_USE)
		{
			startelem = i;
			break;
		}

		hash = SH_ENTRY_HASH(tb, oldentry);
		optimal = SH_INITIAL_BUCKET(tb, hash);

		if (optimal == i)
		{
			startelem = i;
			break;
		}
	}

	/* and copy all elements in the old table */
	copyelem = startelem;
	for (i = 0; i < oldsize; i++)
	{
		SH_ELEMENT_TYPE *oldentry = &olddata[copyelem];

		if (oldentry->status == SH_STATUS_IN_USE)
		{
			uint32 hash;
			uint32 startelem;
			uint32 curelem;
			SH_ELEMENT_TYPE *newentry;

			hash = SH_ENTRY_HASH(tb, oldentry);
			startelem = SH_INITIAL_BUCKET(tb, hash);
			curelem = startelem;

			/* find empty element to put data into */
			while (true)
			{
				newentry = &newdata[curelem];

				if (newentry->status == SH_STATUS_EMPTY)
				{
					break;
				}

				curelem = SH_NEXT(tb, curelem, startelem);
			}

			/* copy entry to new slot */
			memcpy(newentry, oldentry, sizeof(SH_ELEMENT_TYPE));
		}

		/* can't use SH_NEXT here, would use new size */
		copyelem++;
		if (copyelem >= oldsize)
		{
			copyelem = 0;
		}
	}

	SH_FREE(tb, olddata);
}

/*
 * Insert the key key into the hash-table, set *found to true if the key
 * already exists, false otherwise. Returns the hash-table entry in either
 * case.
 */
SH_SCOPE SH_ELEMENT_TYPE *
SH_INSERT(SH_TYPE *tb, SH_KEY_TYPE key, bool *found)
{
	uint32 hash = SH_HASH_KEY(tb, key);
	uint32 startelem;
	uint32 curelem;
	SH_ELEMENT_TYPE *data;
	uint32 insertdist;

restart:
	insertdist = 0;

	/*
	 * We do the grow check even if the key is actually present, to avoid
	 * doing the check inside the loop. This also lets us avoid having to
	 * re-find our position in the hashtable after resizing.
	 *
	 * Note that this also reached when resizing the table due to
	 * SH_GROW_MAX_DIB / SH_GROW_MAX_MOVE.
	 */
	if (unlikely(tb->members >= tb->grow_threshold))
	{
		if (tb->size == SH_MAX_SIZE)
		{
			elog(ERROR, "hash table size exceeded");
		}

		/*
		 * When optimizing, it can be very useful to print these out.
		 */
		/* SH_STAT(tb); */
		SH_GROW(tb, tb->size * 2);
		/* SH_STAT(tb); */
	}

	/* perform insert, start bucket search at optimal location */
	data = tb->data;
	startelem = SH_INITIAL_BUCKET(tb, hash);
	curelem = startelem;
	while (true)
	{
		uint32 curdist;
		uint32 curhash;
		uint32 curoptimal;
		SH_ELEMENT_TYPE *entry = &data[curelem];

		/* any empty bucket can directly be used */
		if (entry->status == SH_STATUS_EMPTY)
		{
			tb->members++;
			entry->SH_KEY = key;
#ifdef SH_STORE_HASH
			SH_GET_HASH(tb, entry) = hash;
#endif
			entry->status = SH_STATUS_IN_USE;
			*found = false;
			return entry;
		}

		/*
		 * If the bucket is not empty, we either found a match (in which case
		 * we're done), or we have to decide whether to skip over or move the
		 * colliding entry. When the colliding element's distance to its
		 * optimal position is smaller than the to-be-inserted entry's, we
		 * shift the colliding entry (and its followers) forward by one.
		 */

		if (SH_COMPARE_KEYS(tb, hash, key, entry))
		{
			Assert(entry->status == SH_STATUS_IN_USE);
			*found = true;
			return entry;
		}

		curhash = SH_ENTRY_HASH(tb, entry);
		curoptimal = SH_INITIAL_BUCKET(tb, curhash);
		curdist = SH_DISTANCE_FROM_OPTIMAL(tb, curoptimal, curelem);

		if (insertdist > curdist)
		{
			SH_ELEMENT_TYPE *lastentry = entry;
			uint32 emptyelem = curelem;
			uint32 moveelem;
			int32 emptydist = 0;

			/* find next empty bucket */
			while (true)
			{
				SH_ELEMENT_TYPE *emptyentry;

				emptyelem = SH_NEXT(tb, emptyelem, startelem);
				emptyentry = &data[emptyelem];

				if (emptyentry->status == SH_STATUS_EMPTY)
				{
					lastentry = emptyentry;
					break;
				}

				/*
				 * To avoid negative consequences from overly imbalanced
				 * hashtables, grow the hashtable if collisions would require
				 * us to move a lot of entries.  The most likely cause of such
				 * imbalance is filling a (currently) small table, from a
				 * currently big one, in hash-table order.  Don't grow if the
				 * hashtable would be too empty, to prevent quick space
				 * explosion for some weird edge cases.
				 */
				if (unlikely(++emptydist > SH_GROW_MAX_MOVE) &&
					((double) tb->members / tb->size) >= SH_GROW_MIN_FILLFACTOR)
				{
					tb->grow_threshold = 0;
					goto restart;
				}
			}

			/* shift forward, starting at last occupied element */

			/*
			 * TODO: This could be optimized to be one memcpy in may cases,
			 * excepting wrapping around at the end of ->data. Hasn't shown up
			 * in profiles so far though.
			 */
			moveelem = emptyelem;
			while (moveelem != curelem)
			{
				SH_ELEMENT_TYPE *moveentry;

				moveelem = SH_PREV(tb, moveelem, startelem);
				moveentry = &data[moveelem];

				memcpy(lastentry, moveentry, sizeof(SH_ELEMENT_TYPE));
				lastentry = moveentry;
			}

			/* and fill the now empty spot */
			tb->members++;

			entry->SH_KEY = key;
#ifdef SH_STORE_HASH
			SH_GET_HASH(tb, entry) = hash;
#endif
			entry->status = SH_STATUS_IN_USE;
			*found = false;
			return entry;
		}

		curelem = SH_NEXT(tb, curelem, startelem);
		insertdist++;

		/*
		 * To avoid negative consequences from overly imbalanced hashtables,
		 * grow the hashtable if collisions lead to large runs. The most
		 * likely cause of such imbalance is filling a (currently) small
		 * table, from a currently big one, in hash-table order.  Don't grow
		 * if the hashtable would be too empty, to prevent quick space
		 * explosion for some weird edge cases.
		 */
		if (unlikely(insertdist > SH_GROW_MAX_DIB) &&
			((double) tb->members / tb->size) >= SH_GROW_MIN_FILLFACTOR)
		{
			tb->grow_threshold = 0;
			goto restart;
		}
	}
}

/*
 * Lookup up entry in hash table.  Returns NULL if key not present.
 */
SH_SCOPE SH_ELEMENT_TYPE *
SH_LOOKUP(SH_TYPE *tb, SH_KEY_TYPE key)
{
	uint32 hash = SH_HASH_KEY(tb, key);
	const uint32 startelem = SH_INITIAL_BUCKET(tb, hash);
	uint32 curelem = startelem;

	while (true)
	{
		SH_ELEMENT_TYPE *entry = &tb->data[curelem];

		if (entry->status == SH_STATUS_EMPTY)
		{
			return NULL;
		}

		Assert(entry->status == SH_STATUS_IN_USE);

		if (SH_COMPARE_KEYS(tb, hash, key, entry))
			return entry;

		/*
		 * TODO: we could stop search based on distance. If the current
		 * buckets's distance-from-optimal is smaller than what we've skipped
		 * already, the entry doesn't exist. Probably only do so if
		 * SH_STORE_HASH is defined, to avoid re-computing hashes?
		 */

		curelem = SH_NEXT(tb, curelem, startelem);
	}
}

/*
 * Delete entry from hash table.  Returns whether to-be-deleted key was
 * present.
 */
SH_SCOPE bool
SH_DELETE(SH_TYPE *tb, SH_KEY_TYPE key)
{
	uint32 hash = SH_HASH_KEY(tb, key);
	uint32 startelem = SH_INITIAL_BUCKET(tb, hash);
	uint32 curelem = startelem;

	while (true)
	{
		SH_ELEMENT_TYPE *entry = &tb->data[curelem];

		if (entry->status == SH_STATUS_EMPTY)
			return false;

		if (entry->status == SH_STATUS_IN_USE && SH_COMPARE_KEYS(tb, hash, key, entry))
		{
			SH_ELEMENT_TYPE *lastentry = entry;

			tb->members--;

			/*
			 * Backward shift following elements till either an empty element
			 * or an element at its optimal position is encountered.
			 *
			 * While that sounds expensive, the average chain length is short,
			 * and deletions would otherwise require tombstones.
			 */
			while (true)
			{
				SH_ELEMENT_TYPE *curentry;
				uint32 curhash;
				uint32 curoptimal;

				curelem = SH_NEXT(tb, curelem, startelem);
				curentry = &tb->data[curelem];

				if (curentry->status != SH_STATUS_IN_USE)
				{
					lastentry->status = SH_STATUS_EMPTY;
					break;
				}

				curhash = SH_ENTRY_HASH(tb, curentry);
				curoptimal = SH_INITIAL_BUCKET(tb, curhash);

				/* current is at optimal position, done */
				if (curoptimal == curelem)
				{
					lastentry->status = SH_STATUS_EMPTY;
					break;
				}

				/* shift */
				memcpy(lastentry, curentry, sizeof(SH_ELEMENT_TYPE));

				lastentry = curentry;
			}

			return true;
		}

		/* TODO: return false; if distance too big */

		curelem = SH_NEXT(tb, curelem, startelem);
	}
}

/*
 * Initialize iterator.
 */
SH_SCOPE void
SH_START_ITERATE(SH_TYPE *tb, SH_ITERATOR *iter)
{
	int i;
	uint64 startelem = PG_UINT64_MAX;

	/*
	 * Search for the first empty element. As deletions during iterations are
	 * supported, we want to start/end at an element that cannot be affected
	 * by elements being shifted.
	 */
	for (i = 0; i < tb->size; i++)
	{
		SH_ELEMENT_TYPE *entry = &tb->data[i];

		if (entry->status != SH_STATUS_IN_USE)
		{
			startelem = i;
			break;
		}
	}

	Assert(startelem < SH_MAX_SIZE);

	/*
	 * Iterate backwards, that allows the current element to be deleted, even
	 * if there are backward shifts
	 */
	iter->cur = startelem;
	iter->end = iter->cur;
	iter->done = false;
}

/*
 * Initialize iterator to a specific bucket. That's really only useful for
 * cases where callers are partially iterating over the hashspace, and that
 * iteration deletes and inserts elements based on visited entries. Doing that
 * repeatedly could lead to an unbalanced keyspace when always starting at the
 * same position.
 */
SH_SCOPE void
SH_START_ITERATE_AT(SH_TYPE *tb, SH_ITERATOR *iter, uint32 at)
{
	/*
	 * Iterate backwards, that allows the current element to be deleted, even
	 * if there are backward shifts.
	 */
	iter->cur = at & tb->sizemask; /* ensure at is within a valid range */
	iter->end = iter->cur;
	iter->done = false;
}

/*
 * Iterate over all entries in the hash-table. Return the next occupied entry,
 * or NULL if done.
 *
 * During iteration the current entry in the hash table may be deleted,
 * without leading to elements being skipped or returned twice.  Additionally
 * the rest of the table may be modified (i.e. there can be insertions or
 * deletions), but if so, there's neither a guarantee that all nodes are
 * visited at least once, nor a guarantee that a node is visited at most once.
 */
SH_SCOPE SH_ELEMENT_TYPE *
SH_ITERATE(SH_TYPE *tb, SH_ITERATOR *iter)
{
	while (!iter->done)
	{
		SH_ELEMENT_TYPE *elem;

		elem = &tb->data[iter->cur];

		/* next element in backward direction */
		iter->cur = (iter->cur - 1) & tb->sizemask;

		if ((iter->cur & tb->sizemask) == (iter->end & tb->sizemask))
			iter->done = true;
		if (elem->status == SH_STATUS_IN_USE)
		{
			return elem;
		}
	}

	return NULL;
}

/*
 * Report some statistics about the state of the hashtable. For
 * debugging/profiling purposes only.
 */
SH_SCOPE void
SH_STAT(SH_TYPE *tb)
{
	uint32 max_chain_length = 0;
	uint32 total_chain_length = 0;
	double avg_chain_length;
	double fillfactor;
	uint32 i;

	uint32 *collisions = palloc0(tb->size * sizeof(uint32));
	uint32 total_collisions = 0;
	uint32 max_collisions = 0;
	double avg_collisions;

	for (i = 0; i < tb->size; i++)
	{
		uint32 hash;
		uint32 optimal;
		uint32 dist;
		SH_ELEMENT_TYPE *elem;

		elem = &tb->data[i];

		if (elem->status != SH_STATUS_IN_USE)
			continue;

		hash = SH_ENTRY_HASH(tb, elem);
		optimal = SH_INITIAL_BUCKET(tb, hash);
		dist = SH_DISTANCE_FROM_OPTIMAL(tb, optimal, i);

		if (dist > max_chain_length)
			max_chain_length = dist;
		total_chain_length += dist;

		collisions[optimal]++;
	}

	for (i = 0; i < tb->size; i++)
	{
		uint32 curcoll = collisions[i];

		if (curcoll == 0)
			continue;

		/* single contained element is not a collision */
		curcoll--;
		total_collisions += curcoll;
		if (curcoll > max_collisions)
			max_collisions = curcoll;
	}

	if (tb->members > 0)
	{
		fillfactor = tb->members / ((double) tb->size);
		avg_chain_length = ((double) total_chain_length) / tb->members;
		avg_collisions = ((double) total_collisions) / tb->members;
	}
	else
	{
		fillfactor = 0;
		avg_chain_length = 0;
		avg_collisions = 0;
	}

	elog(LOG,
		 "size: " UINT64_FORMAT
		 ", members: %u, filled: %f, total chain: %u, max chain: %u, avg chain: %f, "
		 "total_collisions: %u, max_collisions: %i, avg_collisions: %f",
		 tb->size,
		 tb->members,
		 fillfactor,
		 total_chain_length,
		 max_chain_length,
		 avg_chain_length,
		 total_collisions,
		 max_collisions,
		 avg_collisions);
}

#endif /* SH_DEFINE */

/* undefine external parameters, so next hash table can be defined */
#undef SH_PREFIX
#undef SH_KEY_TYPE
#undef SH_KEY
#undef SH_ELEMENT_TYPE
#undef SH_HASH_KEY
#undef SH_SCOPE
#undef SH_DECLARE
#undef SH_DEFINE
#undef SH_GET_HASH
#undef SH_STORE_HASH
#undef SH_USE_NONDEFAULT_ALLOCATOR
#undef SH_EQUAL

/* undefine locally declared macros */
#undef SH_MAKE_PREFIX
#undef SH_MAKE_NAME
#undef SH_MAKE_NAME_
#undef SH_FILLFACTOR
#undef SH_MAX_FILLFACTOR
#undef SH_GROW_MAX_DIB
#undef SH_GROW_MAX_MOVE
#undef SH_GROW_MIN_FILLFACTOR
#undef SH_MAX_SIZE

/* types */
#undef SH_TYPE
#undef SH_STATUS
#undef SH_STATUS_EMPTY
#undef SH_STATUS_IN_USE
#undef SH_ITERATOR

/* external function names */
#undef SH_CREATE
#undef SH_DESTROY
#undef SH_RESET
#undef SH_INSERT
#undef SH_DELETE
#undef SH_LOOKUP
#undef SH_GROW
#undef SH_START_ITERATE
#undef SH_START_ITERATE_AT
#undef SH_ITERATE
#undef SH_ALLOCATE
#undef SH_FREE
#undef SH_STAT

/* internal function names */
#undef SH_COMPUTE_PARAMETERS
#undef SH_COMPARE_KEYS
#undef SH_INITIAL_BUCKET
#undef SH_NEXT
#undef SH_PREV
#undef SH_DISTANCE_FROM_OPTIMAL
#undef SH_ENTRY_HASH
