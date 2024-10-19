/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * This file contains source code that was copied and/or modified from
 * the PostgreSQL database, which is licensed under the open-source
 * PostgreSQL License. Please see the NOTICE at the top level
 * directory for a copy of the PostgreSQL License.
 */

/*
 * simplehash.h
 *
 *	  When included this file generates a "templated" (by way of macros)
 *	  open-addressing hash table implementation specialized to user-defined
 *	  types.
 *
 *	  It's probably not worthwhile to generate such a specialized implementation
 *	  for hash tables that aren't performance or space sensitive.
 *
 *	  Compared to dynahash, simplehash has the following benefits:
 *
 *	  - Due to the "templated" code generation has known structure sizes and no
 *	    indirect function calls (which show up substantially in dynahash
 *	    profiles). These features considerably increase speed for small
 *	    entries.
 *	  - Open addressing has better CPU cache behavior than dynahash's chained
 *	    hashtables.
 *	  - The generated interface is type-safe and easier to use than dynahash,
 *	    though at the cost of more complex setup.
 *	  - Allocates memory in a MemoryContext or another allocator with a
 *	    malloc/free style interface (which isn't easily usable in a shared
 *	    memory context)
 *	  - Does not require the overhead of a separate memory context.
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
 *	  - SH_RAW_ALLOCATOR - if defined, memory contexts are not used; instead,
 *	    use this to allocate bytes. The allocator must zero the returned space.
 *	  - SH_USE_NONDEFAULT_ALLOCATOR - if defined no element allocator functions
 *		are defined, so you can supply your own
 *	  The following parameters are only relevant when SH_DEFINE is defined:
 *	  - SH_KEY - name of the element in SH_ELEMENT_TYPE containing the hash key
 *	  - SH_EQUAL(table, a, b) - compare two table keys
 *	  - SH_HASH_KEY(table, key) - generate hash for the key
 *	  - SH_STORE_HASH - if defined the hash is stored in the elements
 *	  - SH_GET_HASH(tb, a) - return the field to store the hash in
 *
 *	  While SH_STORE_HASH (and subsequently SH_GET_HASH) are optional, because
 *	  the hash table implementation needs to compare hashes to move elements
 *	  (particularly when growing the hash), it's preferable, if possible, to
 *	  store the element's hash in the element's data type. If the hash is so
 *	  stored, the hash table will also compare hashes before calling SH_EQUAL
 *	  when comparing two keys.
 *
 *	  For convenience the hash table create functions accept a void pointer
 *	  that will be stored in the hash table type's member private_data. This
 *	  allows callbacks to reference caller provided data.
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
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/lib/simplehash.h
 */

#include "port/pg_bitutils.h"

/* helpers */
#define SH_MAKE_PREFIX(a) CppConcat(a, _)
#define SH_MAKE_NAME(name) SH_MAKE_NAME_(SH_MAKE_PREFIX(SH_PREFIX), name)
#define SH_MAKE_NAME_(a, b) CppConcat(a, b)

/* name macros for: */

/* type declarations */
#define SH_TYPE SH_MAKE_NAME(hash)
#define SH_ITERATOR SH_MAKE_NAME(iterator)

/* function declarations */
#define SH_CREATE SH_MAKE_NAME(create)
#define SH_DESTROY SH_MAKE_NAME(destroy)
#define SH_RESET SH_MAKE_NAME(reset)
#define SH_INSERT SH_MAKE_NAME(insert)
#define SH_INSERT_HASH SH_MAKE_NAME(insert_hash)
#define SH_LOOKUP SH_MAKE_NAME(lookup)
#define SH_LOOKUP_HASH SH_MAKE_NAME(lookup_hash)
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
#define SH_INSERT_HASH_INTERNAL SH_MAKE_NAME(insert_hash_internal)
#define SH_LOOKUP_HASH_INTERNAL SH_MAKE_NAME(lookup_hash_internal)

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
	SH_ELEMENT_TYPE *restrict data;

#ifndef SH_RAW_ALLOCATOR
	/* memory context to use for allocations */
	MemoryContext ctx;
#endif

	/* user defined data, useful for callbacks */
	void *private_data;
} SH_TYPE;

typedef struct SH_ITERATOR
{
	uint32 cur; /* current element */
	uint32 end;
	bool done; /* iterator exhausted? */
} SH_ITERATOR;

/* externally visible function prototypes */
#ifdef SH_RAW_ALLOCATOR
/* <prefix>_hash <prefix>_create(uint32 nelements, void *private_data) */
SH_SCOPE SH_TYPE *SH_CREATE(uint32 nelements, void *private_data);
#else
/*
 * <prefix>_hash <prefix>_create(MemoryContext ctx, uint32 nelements,
 *								 void *private_data)
 */
SH_SCOPE SH_TYPE *SH_CREATE(MemoryContext ctx, uint32 nelements, void *private_data);
#endif

/* void <prefix>_destroy(<prefix>_hash *tb) */
SH_SCOPE void SH_DESTROY(SH_TYPE *tb);

/* void <prefix>_reset(<prefix>_hash *tb) */
SH_SCOPE void SH_RESET(SH_TYPE *tb);

/* void <prefix>_grow(<prefix>_hash *tb, uint64 newsize) */
SH_SCOPE void SH_GROW(SH_TYPE *tb, uint64 newsize);

/* <element> *<prefix>_insert(<prefix>_hash *tb, <key> key, bool *found) */
SH_SCOPE SH_ELEMENT_TYPE *SH_INSERT(SH_TYPE *tb, SH_KEY_TYPE key, bool *found);

/*
 * <element> *<prefix>_insert_hash(<prefix>_hash *tb, <key> key, uint32 hash,
 * 								  bool *found)
 */
SH_SCOPE SH_ELEMENT_TYPE *SH_INSERT_HASH(SH_TYPE *tb, SH_KEY_TYPE key, uint32 hash, bool *found);

/* <element> *<prefix>_lookup(<prefix>_hash *tb, <key> key) */
SH_SCOPE SH_ELEMENT_TYPE *SH_LOOKUP(SH_TYPE *tb, SH_KEY_TYPE key);

/* <element> *<prefix>_lookup_hash(<prefix>_hash *tb, <key> key, uint32 hash) */
SH_SCOPE SH_ELEMENT_TYPE *SH_LOOKUP_HASH(SH_TYPE *tb, SH_KEY_TYPE key, uint32 hash);

/* void <prefix>_start_iterate(<prefix>_hash *tb, <prefix>_iterator *iter) */
SH_SCOPE void SH_START_ITERATE(SH_TYPE *tb, SH_ITERATOR *iter);

/*
 * void <prefix>_start_iterate_at(<prefix>_hash *tb, <prefix>_iterator *iter,
 *								  uint32 at)
 */
SH_SCOPE void SH_START_ITERATE_AT(SH_TYPE *tb, SH_ITERATOR *iter, uint32 at);

/* <element> *<prefix>_iterate(<prefix>_hash *tb, <prefix>_iterator *iter) */
SH_SCOPE SH_ELEMENT_TYPE *SH_ITERATE(SH_TYPE *tb, SH_ITERATOR *iter);

/* void <prefix>_stat(<prefix>_hash *tb */
SH_SCOPE void SH_STAT(SH_TYPE *tb);

#endif /* SH_DECLARE */

/* generate implementation of the hash table */
#ifdef SH_DEFINE

#ifndef SH_RAW_ALLOCATOR
#include "utils/memutils.h"
#endif

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

#ifdef FRONTEND
#define sh_error(...) pg_fatal(__VA_ARGS__)
#define sh_log(...) pg_log_info(__VA_ARGS__)
#else
#define sh_error(...) elog(ERROR, __VA_ARGS__)
#define sh_log(...) elog(LOG, __VA_ARGS__)
#endif

#endif

/*
 * Compute sizing parameters for hashtable. Called when creating and growing
 * the hashtable.
 */
static inline void
SH_COMPUTE_PARAMETERS(SH_TYPE *tb, uint64 newsize)
{
	uint64 size;

	/* supporting zero sized hashes would complicate matters */
	size = Max(newsize, 2);

	/* round up size to the next power of 2, that's how bucketing works */
	size = pg_nextpower2_64(size);
	Assert(size <= SH_MAX_SIZE);

	/*
	 * Verify that allocation of ->data is possible on this platform, without
	 * overflowing Size.
	 */
	if (unlikely((((uint64) sizeof(SH_ELEMENT_TYPE)) * size) >= SIZE_MAX / 2))
		sh_error("hash table too large");

	/* now set size */
	tb->size = size;
	tb->sizemask = (uint32) (size - 1);

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
static pg_attribute_always_inline uint32
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
#ifdef SH_RAW_ALLOCATOR
	return SH_RAW_ALLOCATOR(size);
#else
	return MemoryContextAllocExtended(type->ctx, size, MCXT_ALLOC_HUGE | MCXT_ALLOC_ZERO);
#endif
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
#ifdef SH_RAW_ALLOCATOR
SH_SCOPE SH_TYPE *
SH_CREATE(uint32 nelements, void *private_data)
#else
SH_SCOPE SH_TYPE *
SH_CREATE(MemoryContext ctx, uint32 nelements, void *private_data)
#endif
{
	SH_TYPE *tb;
	uint64 size;

#ifdef SH_RAW_ALLOCATOR
	tb = (SH_TYPE *) SH_RAW_ALLOCATOR(sizeof(SH_TYPE));
#else
	tb = (SH_TYPE *) MemoryContextAllocZero(ctx, sizeof(SH_TYPE));
	tb->ctx = ctx;
#endif
	tb->private_data = private_data;

	/* increase nelements by fillfactor, want to store nelements elements */
	size = Min((double) SH_MAX_SIZE, ((double) nelements) / SH_FILLFACTOR);

	SH_COMPUTE_PARAMETERS(tb, size);

	tb->data = (SH_ELEMENT_TYPE *) SH_ALLOCATE(tb, sizeof(SH_ELEMENT_TYPE) * tb->size);

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
SH_GROW(SH_TYPE *tb, uint64 newsize)
{
	uint64 oldsize = tb->size;
	SH_ELEMENT_TYPE *olddata = tb->data;
	SH_ELEMENT_TYPE *newdata;
	uint32 i;
	uint32 startelem = 0;
	uint32 copyelem;

	Assert(oldsize == pg_nextpower2_64(oldsize));
	Assert(oldsize != SH_MAX_SIZE);
	Assert(oldsize < newsize);

	/* compute parameters for new table */
	SH_COMPUTE_PARAMETERS(tb, newsize);

	tb->data = (SH_ELEMENT_TYPE *) SH_ALLOCATE(tb, sizeof(SH_ELEMENT_TYPE) * tb->size);

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

		if (SH_ENTRY_EMPTY(oldentry))
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

		if (!SH_ENTRY_EMPTY(oldentry))
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

				if (SH_ENTRY_EMPTY(newentry))
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
 * This is a separate static inline function, so it can be reliably be inlined
 * into its wrapper functions even if SH_SCOPE is extern.
 */
static pg_attribute_always_inline SH_ELEMENT_TYPE *
SH_INSERT_HASH_INTERNAL(SH_TYPE *restrict tb, SH_KEY_TYPE key, uint32 hash, bool *found)
{
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
		if (unlikely(tb->size == SH_MAX_SIZE))
			sh_error("hash table size exceeded");

		/*
		 * When optimizing, it can be very useful to print these out.
		 */
		/* SH_STAT(tb); */
		SH_GROW(tb, tb->size * 2);
		/* SH_STAT(tb); */
	}

	SH_ELEMENT_TYPE *restrict data = tb->data;

	/* perform insert, start bucket search at optimal location */
	const uint32 startelem = SH_INITIAL_BUCKET(tb, hash);
	uint32 curelem = startelem;
	uint32 insertdist = 0;
	while (true)
	{
		SH_ELEMENT_TYPE *entry = &data[curelem];

		/* any empty bucket can directly be used */
		if (SH_ENTRY_EMPTY(entry))
		{
			tb->members++;
			entry->SH_KEY = key;
#ifdef SH_STORE_HASH
			SH_GET_HASH(tb, entry) = hash;
#endif
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
			Assert(!SH_ENTRY_EMPTY(entry));
			*found = true;
			return entry;
		}

		const uint32 curhash = SH_ENTRY_HASH(tb, entry);
		const uint32 curoptimal = SH_INITIAL_BUCKET(tb, curhash);
		const uint32 curdist = SH_DISTANCE_FROM_OPTIMAL(tb, curoptimal, curelem);

		if (insertdist > curdist)
		{
			/* We're going to insert at this position. */
			break;
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
			SH_GROW(tb, tb->size * 2);
			return SH_INSERT_HASH(tb, key, hash, found);
		}
	}

	/* Actually insert. */
	SH_ELEMENT_TYPE *entry = &data[curelem];
	SH_ELEMENT_TYPE *lastentry = entry;
	uint32 emptyelem = curelem;
	int32 emptydist = 0;

	/* find next empty bucket */
	while (true)
	{
		SH_ELEMENT_TYPE *emptyentry;

		emptyelem = SH_NEXT(tb, emptyelem, startelem);
		emptyentry = &data[emptyelem];

		if (SH_ENTRY_EMPTY(emptyentry))
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
			SH_GROW(tb, tb->size * 2);
			return SH_INSERT_HASH(tb, key, hash, found);
		}
	}

	/* shift forward, starting at last occupied element */

	/*
	 * TODO: This could be optimized to be one memcpy in many cases,
	 * excepting wrapping around at the end of ->data. Hasn't shown up
	 * in profiles so far though.
	 */
	uint32 moveelem = emptyelem;
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
	*found = false;
	return entry;
}

/*
 * Insert the key key into the hash-table, set *found to true if the key
 * already exists, false otherwise. Returns the hash-table entry in either
 * case.
 */
static pg_attribute_always_inline SH_ELEMENT_TYPE *
SH_INSERT(SH_TYPE *tb, SH_KEY_TYPE key, bool *found)
{
	uint32 hash = SH_HASH_KEY(tb, key);

	return SH_INSERT_HASH_INTERNAL(tb, key, hash, found);
}

/*
 * Insert the key key into the hash-table using an already-calculated
 * hash. Set *found to true if the key already exists, false
 * otherwise. Returns the hash-table entry in either case.
 */
SH_SCOPE SH_ELEMENT_TYPE *
SH_INSERT_HASH(SH_TYPE *tb, SH_KEY_TYPE key, uint32 hash, bool *found)
{
	return SH_INSERT_HASH_INTERNAL(tb, key, hash, found);
}

/*
 * This is a separate static inline function, so it can be reliably be inlined
 * into its wrapper functions even if SH_SCOPE is extern.
 */
static inline SH_ELEMENT_TYPE *
SH_LOOKUP_HASH_INTERNAL(SH_TYPE *tb, SH_KEY_TYPE key, uint32 hash)
{
	const uint32 startelem = SH_INITIAL_BUCKET(tb, hash);
	uint32 curelem = startelem;

	while (true)
	{
		SH_ELEMENT_TYPE *entry = &tb->data[curelem];

		if (SH_ENTRY_EMPTY(entry))
		{
			return NULL;
		}

		Assert(!SH_ENTRY_EMPTY(entry));

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
 * Lookup up entry in hash table.  Returns NULL if key not present.
 */
SH_SCOPE SH_ELEMENT_TYPE *
SH_LOOKUP(SH_TYPE *tb, SH_KEY_TYPE key)
{
	uint32 hash = SH_HASH_KEY(tb, key);

	return SH_LOOKUP_HASH_INTERNAL(tb, key, hash);
}

/*
 * Lookup up entry in hash table using an already-calculated hash.
 *
 * Returns NULL if key not present.
 */
SH_SCOPE SH_ELEMENT_TYPE *
SH_LOOKUP_HASH(SH_TYPE *tb, SH_KEY_TYPE key, uint32 hash)
{
	return SH_LOOKUP_HASH_INTERNAL(tb, key, hash);
}

/*
 * Initialize iterator.
 */
SH_SCOPE void
SH_START_ITERATE(SH_TYPE *tb, SH_ITERATOR *iter)
{
	uint64 startelem = PG_UINT64_MAX;

	/*
	 * Search for the first empty element. As deletions during iterations are
	 * supported, we want to start/end at an element that cannot be affected
	 * by elements being shifted.
	 */
	for (uint32 i = 0; i < tb->size; i++)
	{
		SH_ELEMENT_TYPE *entry = &tb->data[i];

		if (SH_ENTRY_EMPTY(entry))
		{
			startelem = i;
			break;
		}
	}

	/* we should have found an empty element */
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
		if (!SH_ENTRY_EMPTY(elem))
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

	uint32 *collisions = (uint32 *) palloc0(tb->size * sizeof(uint32));
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

		if (SH_ENTRY_EMPTY(elem))
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

	/* large enough to be worth freeing, even if just used for debugging */
	pfree(collisions);

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

	sh_log("size: " UINT64_FORMAT
		   ", members: %u, filled: %f, total chain: %u, max chain: %u, avg chain: %f, "
		   "total_collisions: %u, max_collisions: %u, avg_collisions: %f",
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
#undef SH_ITERATOR

/* external function names */
#undef SH_CREATE
#undef SH_DESTROY
#undef SH_RESET
#undef SH_INSERT
#undef SH_INSERT_HASH
#undef SH_LOOKUP
#undef SH_LOOKUP_HASH
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
#undef SH_INSERT_HASH_INTERNAL
#undef SH_LOOKUP_HASH_INTERNAL
