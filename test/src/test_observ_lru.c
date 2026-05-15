/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/*
 * Unit tests for the chunk-observ LRU cache data-structure operations
 * (ts_stats_chunk_segment_{find_or_insert,lookup}).
 *
 * The DSM/shmem layer is bypassed: we palloc a tiny segment in regular
 * backend memory, set its structural fields, and thread the free list
 * by hand.  No LWLock is needed: find_or_insert and lookup don't acquire
 * the lock themselves (the API contract puts that on the caller), and
 * these tests are single-threaded so there is nothing to protect against.
 */

#include <postgres.h>
#include <fmgr.h>

#include "test_utils.h"

#include "ts_stats/ts_stats_defs.h"
#include "ts_stats/ts_stats_segment.h"

/* Tiny power-of-two segment for the tests. */
#define TEST_NUM_SLOTS 4
#define TEST_NUM_BUCKETS 4

/* The tests pass distinct compressed_relid / uncompressed_relid Oids by
 * convention: uncompressed_relid = compressed_relid + 100000. */
#define UNCOMP_OF(x) ((Oid) ((x) + 100000))

static TsStatsChunkSegment *
build_test_segment(void)
{
	Size sz = ts_stats_chunk_segment_size(TEST_NUM_SLOTS, TEST_NUM_BUCKETS);
	TsStatsChunkSegment *seg = palloc0(sz);

	seg->magic = TS_STATS_SHMEM_MAGIC;
	seg->dboid = InvalidOid;
	seg->num_slots = TEST_NUM_SLOTS;
	seg->num_buckets = TEST_NUM_BUCKETS;
	seg->lru_head = INVALID_IDX;
	seg->lru_tail = INVALID_IDX;

	slot_idx_t *buckets = ts_stats_chunk_buckets(seg);
	TsStatsChunk *slots = ts_stats_chunk_slots(seg);

	for (uint32 b = 0; b < seg->num_buckets; b++)
	{
		buckets[b] = INVALID_IDX;
	}

	/* Mark all slots free, thread them through the free list via lru_next. */
	for (uint32 s = 0; s < seg->num_slots; s++)
	{
		slots[s].compressed_relid = InvalidOid;
		slots[s].lru_next = (slot_idx_t) (s + 1);
	}
	slots[seg->num_slots - 1].lru_next = INVALID_IDX;
	seg->free_head = 0;

	return seg;
}

/*
 * Walk the LRU list head→tail, writing relids visited into out[].
 * Returns the number of slots walked.  out[] must hold TEST_NUM_SLOTS Oids.
 */
static uint32
walk_lru(TsStatsChunkSegment *seg, Oid out[TEST_NUM_SLOTS])
{
	TsStatsChunk *slots = ts_stats_chunk_slots(seg);
	uint32 n = 0;
	slot_idx_t s = seg->lru_head;

	while (s != INVALID_IDX)
	{
		TestAssertTrue(n < TEST_NUM_SLOTS);
		out[n++] = slots[s].compressed_relid;
		s = slots[s].lru_next;
	}
	return n;
}

/*
 * Find two distinct relids that map to the same hash bucket with
 * TEST_NUM_BUCKETS = 4.  Returns true on success.  A collision among
 * 4 buckets is found within a handful of relids.
 */
static bool
find_colliding_pair(Oid *a, Oid *b)
{
	Oid first_seen[TEST_NUM_BUCKETS];

	for (uint32 i = 0; i < TEST_NUM_BUCKETS; i++)
	{
		first_seen[i] = InvalidOid;
	}

	for (Oid r = 1; r < 1000; r++)
	{
		uint32 bucket = ((uint32) r * 0x9E3779B9U) & (TEST_NUM_BUCKETS - 1);

		if (OidIsValid(first_seen[bucket]))
		{
			*a = first_seen[bucket];
			*b = r;
			return true;
		}
		first_seen[bucket] = r;
	}
	return false;
}

/* ---------- tests ---------- */

static void
test_insert_and_lookup(void)
{
	TsStatsChunkSegment *seg = build_test_segment();

	/* First insert returns a valid slot. */
	slot_idx_t s1 = ts_stats_chunk_segment_find_or_insert(seg, 1001, UNCOMP_OF(1001));
	TestAssertTrue(s1 != INVALID_IDX);

	/* Re-insert returns the same slot. */
	slot_idx_t s2 = ts_stats_chunk_segment_find_or_insert(seg, 1001, UNCOMP_OF(1001));
	TestAssertInt64Eq(s1, s2);

	/* Lookup on a present chunk returns its slot. */
	TestAssertInt64Eq(ts_stats_chunk_segment_lookup(seg, 1001), s1);

	/* Lookup on a missing chunk returns INVALID_IDX (no allocation). */
	TestAssertInt64Eq(ts_stats_chunk_segment_lookup(seg, 9999), INVALID_IDX);

	pfree(seg);
}

static void
test_invalid_oid(void)
{
	TsStatsChunkSegment *seg = build_test_segment();

	/* Either arg invalid → INVALID_IDX. */
	TestAssertInt64Eq(ts_stats_chunk_segment_find_or_insert(seg, InvalidOid, UNCOMP_OF(1)),
					  INVALID_IDX);
	TestAssertInt64Eq(ts_stats_chunk_segment_find_or_insert(seg, 1, InvalidOid), INVALID_IDX);
	TestAssertInt64Eq(ts_stats_chunk_segment_lookup(seg, InvalidOid), INVALID_IDX);

	/* Segment must remain empty: no slot was consumed. */
	TestAssertInt64Eq(seg->lru_head, INVALID_IDX);
	TestAssertInt64Eq(seg->lru_tail, INVALID_IDX);

	pfree(seg);
}

static void
test_lru_ordering(void)
{
	TsStatsChunkSegment *seg = build_test_segment();
	Oid order[TEST_NUM_SLOTS];

	ts_stats_chunk_segment_find_or_insert(seg, 1, UNCOMP_OF(1));
	ts_stats_chunk_segment_find_or_insert(seg, 2, UNCOMP_OF(2));
	ts_stats_chunk_segment_find_or_insert(seg, 3, UNCOMP_OF(3));

	/* Most-recent at head, oldest at tail. */
	uint32 n = walk_lru(seg, order);
	TestAssertInt64Eq(n, 3);
	TestAssertInt64Eq(order[0], 3);
	TestAssertInt64Eq(order[1], 2);
	TestAssertInt64Eq(order[2], 1);

	/* Re-inserting a tail-side entry promotes it to head. */
	ts_stats_chunk_segment_find_or_insert(seg, 1, UNCOMP_OF(1));
	n = walk_lru(seg, order);
	TestAssertInt64Eq(n, 3);
	TestAssertInt64Eq(order[0], 1);
	TestAssertInt64Eq(order[1], 3);
	TestAssertInt64Eq(order[2], 2);

	/* Re-inserting the current head is a no-op for ordering. */
	ts_stats_chunk_segment_find_or_insert(seg, 1, UNCOMP_OF(1));
	n = walk_lru(seg, order);
	TestAssertInt64Eq(n, 3);
	TestAssertInt64Eq(order[0], 1);
	TestAssertInt64Eq(order[1], 3);
	TestAssertInt64Eq(order[2], 2);

	/* Re-inserting a middle entry promotes it to head. */
	ts_stats_chunk_segment_find_or_insert(seg, 2, UNCOMP_OF(2));
	n = walk_lru(seg, order);
	TestAssertInt64Eq(n, 3);
	TestAssertInt64Eq(order[0], 2);
	TestAssertInt64Eq(order[1], 1);
	TestAssertInt64Eq(order[2], 3);

	pfree(seg);
}

static void
test_eviction(void)
{
	TsStatsChunkSegment *seg = build_test_segment();
	Oid order[TEST_NUM_SLOTS];

	/* Fill the cache. */
	ts_stats_chunk_segment_find_or_insert(seg, 1, UNCOMP_OF(1));
	ts_stats_chunk_segment_find_or_insert(seg, 2, UNCOMP_OF(2));
	ts_stats_chunk_segment_find_or_insert(seg, 3, UNCOMP_OF(3));
	ts_stats_chunk_segment_find_or_insert(seg, 4, UNCOMP_OF(4));

	/* Free list is now empty. */
	TestAssertInt64Eq(seg->free_head, INVALID_IDX);

	/* Inserting a 5th relid evicts the LRU tail (relid=1). */
	slot_idx_t s5 = ts_stats_chunk_segment_find_or_insert(seg, 5, UNCOMP_OF(5));
	TestAssertTrue(s5 != INVALID_IDX);

	/* Evicted entry is no longer findable. */
	TestAssertInt64Eq(ts_stats_chunk_segment_lookup(seg, 1), INVALID_IDX);

	/* Order is now 5, 4, 3, 2 — head to tail. */
	uint32 n = walk_lru(seg, order);
	TestAssertInt64Eq(n, 4);
	TestAssertInt64Eq(order[0], 5);
	TestAssertInt64Eq(order[1], 4);
	TestAssertInt64Eq(order[2], 3);
	TestAssertInt64Eq(order[3], 2);

	/* Promoting the new tail (2) and inserting another fresh chunk
	 * evicts the new tail (3), not 2. */
	ts_stats_chunk_segment_find_or_insert(seg, 2, UNCOMP_OF(2));
	ts_stats_chunk_segment_find_or_insert(seg, 6, UNCOMP_OF(6));

	TestAssertInt64Eq(ts_stats_chunk_segment_lookup(seg, 3), INVALID_IDX);
	TestAssertTrue(ts_stats_chunk_segment_lookup(seg, 2) != INVALID_IDX);

	pfree(seg);
}

static void
test_hash_collision_chain(void)
{
	TsStatsChunkSegment *seg = build_test_segment();

	Oid a, b;
	TestAssertBoolEq(find_colliding_pair(&a, &b), true);

	slot_idx_t s_a = ts_stats_chunk_segment_find_or_insert(seg, a, UNCOMP_OF(a));
	slot_idx_t s_b = ts_stats_chunk_segment_find_or_insert(seg, b, UNCOMP_OF(b));

	/* Both inserted, in different slots, on the same bucket chain. */
	TestAssertTrue(s_a != INVALID_IDX);
	TestAssertTrue(s_b != INVALID_IDX);
	TestAssertTrue(s_a != s_b);

	/* Both findable — exercises chain walk in hash_lookup. */
	TestAssertInt64Eq(ts_stats_chunk_segment_lookup(seg, a), s_a);
	TestAssertInt64Eq(ts_stats_chunk_segment_lookup(seg, b), s_b);

	/* Push enough new chunks through to evict at least one of {a, b}.
	 * Whichever survives must still be findable — i.e. hash_unlink must
	 * have removed only the evicted one from the chain, not both. */
	ts_stats_chunk_segment_find_or_insert(seg, 7000, UNCOMP_OF(7000));
	ts_stats_chunk_segment_find_or_insert(seg, 7001, UNCOMP_OF(7001));
	ts_stats_chunk_segment_find_or_insert(seg, 7002, UNCOMP_OF(7002));

	slot_idx_t found_a = ts_stats_chunk_segment_lookup(seg, a);
	slot_idx_t found_b = ts_stats_chunk_segment_lookup(seg, b);
	TestAssertTrue(found_a == INVALID_IDX || found_b == INVALID_IDX);
	TestAssertTrue(found_a != INVALID_IDX || found_b != INVALID_IDX);

	pfree(seg);
}

TS_TEST_FN(ts_test_observ_lru)
{
	test_insert_and_lookup();
	test_invalid_oid();
	test_lru_ordering();
	test_eviction();
	test_hash_collision_chain();
	PG_RETURN_VOID();
}
