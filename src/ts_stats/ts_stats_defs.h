/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include "export.h"
#include <chunk_insert_state.h>
#include <datatype/timestamp.h>
#include <storage/lwlock.h>

#include <sys/time.h>

/*
 * Chunk-centric LRU cache.
 *
 * This is a mixed data structure where the LRU properties are
 * maintained by a doubly-linked list of slots. Point accesses to the
 * entries are via a hash table that maps chunk compressed_relid to slot index.
 *
 * The hash table is an external bucket array of slot indices, with
 * chaining via a "hash_next" field.
 *
 * The LRU list is a doubly-linked list of slots, with "lru_prev" and
 * "lru_next" fields.  The lru_next field is also used to chain free
 * slots together when not in use.
 *
 * Because the data structure is located in the shared memory the linking
 * between entries are done via slot indices instead of pointers.  The slot
 * index is a uint16 (slot_idx_t), so the maximum number of entries is 65535.
 *
 * INVALID_IDX = 0xFFFF means "no slot" / chain terminator.
 *
 * The three intrusive lists thread through the slot array:
 *  - hash chain  via hash_next                           (find by compressed_relid)
 *  - LRU list    via lru_prev / lru_next                 (eviction order)
 *  - free list   via lru_next reused on free slots       (reusable slots)
 */

typedef uint16 slot_idx_t;
#define INVALID_IDX ((slot_idx_t) 0xFFFF)
#define TS_STATS_SHMEM_MAGIC 0x54534F42 /* "TSOB" — initialized-segment latch */

typedef struct MinMaxSumData
{
	double sqsum;
	uint64 sum_val;
	uint32 min_val;
	uint32 max_val;
} MinMaxSumData;

static inline void
init_min_max_sum(MinMaxSumData *mmsd)
{
	mmsd->sqsum = 0;
	mmsd->sum_val = 0;
	mmsd->min_val = UINT32_MAX;
	mmsd->max_val = 0;
}

static inline void
mms_observe(MinMaxSumData *m, uint32 value)
{
	if (value < m->min_val)
	{
		m->min_val = value;
	}
	if (value > m->max_val)
	{
		m->max_val = value;
	}
	m->sum_val += value;
	m->sqsum += (double) value * value;
}

static inline void
mms_merge(MinMaxSumData *dst, const MinMaxSumData *src)
{
	if (src->min_val < dst->min_val)
	{
		dst->min_val = src->min_val;
	}
	if (src->max_val > dst->max_val)
	{
		dst->max_val = src->max_val;
	}
	dst->sum_val += src->sum_val;
	dst->sqsum += src->sqsum;
}

/* Struct fields are ordered to minimize padding */
typedef struct TsStatsChunk
{
	/* Microseconds since segment->base_timestamp. */
	uint64 first_update_us;
	uint64 last_update_us;

	/* Chunks are identified by the compressed_relid */
	Oid compressed_relid;
	Oid uncompressed_relid;

	/* Chunk stats at compression, totals: */
	uint32 compressed_batch_count;
	uint32 compressed_block_count;

	/* byte stat across all batches and columns: */
	MinMaxSumData compressed_block_bytes;

	/* sum bytes for each column, across batches: */
	MinMaxSumData compressed_batch_bytes;

	/* rows across batches: */
	MinMaxSumData compressed_batch_rows;

	/* Aggregated Cmd stats */
	SharedCounters cmd_totals;

	/* Cmd stats for the last operation */
	SharedCounters cmd_last_op;

	/* Operation counts */
	uint32 n_selects;
	uint32 n_inserts;
	uint32 n_updates;
	uint32 n_deletes;

	/* Linkage: hashing, LRU, free list */
	slot_idx_t hash_next;
	slot_idx_t lru_prev;
	slot_idx_t lru_next;
} TsStatsChunk;

typedef struct TsStatsChunkSegment
{
	/* Magic set once at init, never written again*/
	uint32 magic; /* TS_STATS_SHMEM_MAGIC when initialized */
	Oid dboid;	  /* database that owns this segment */

	LWLock lock;

	/* base timestamp for *_update_us fields */
	TimestampTz base_timestamp;

	/* Number of slots and hash buckets */
	uint32 num_slots;
	uint32 num_buckets;

	/* Mutable state */
	slot_idx_t lru_head;
	slot_idx_t lru_tail;
	slot_idx_t free_head;

	/*
	 * Trailing arrays — dynamically sized
	 *
	 * The logical layout is:
	 *
	 *   slot_idx_t  buckets[num_buckets];
	 *   TsStatsChunk   slots  [num_slots];
	 */
} TsStatsChunkSegment;

static inline slot_idx_t *
ts_stats_chunk_buckets(TsStatsChunkSegment *seg)
{
	return (slot_idx_t *) ((char *) seg + MAXALIGN(sizeof(TsStatsChunkSegment)));
}

static inline const slot_idx_t *
ts_stats_chunk_buckets_const(const TsStatsChunkSegment *seg)
{
	return (const slot_idx_t *) ((const char *) seg + MAXALIGN(sizeof(TsStatsChunkSegment)));
}

static inline TsStatsChunk *
ts_stats_chunk_slots(TsStatsChunkSegment *seg)
{
	return (TsStatsChunk *) ((char *) seg + MAXALIGN(sizeof(TsStatsChunkSegment)) +
							 MAXALIGN(seg->num_buckets * sizeof(slot_idx_t)));
}

static inline const TsStatsChunk *
ts_stats_chunk_slots_const(const TsStatsChunkSegment *seg)
{
	return (const TsStatsChunk *) ((const char *) seg + MAXALIGN(sizeof(TsStatsChunkSegment)) +
								   MAXALIGN(seg->num_buckets * sizeof(slot_idx_t)));
}

static inline Size
ts_stats_chunk_segment_size(uint32 num_slots, uint32 num_buckets)
{
	return MAXALIGN(sizeof(TsStatsChunkSegment)) + MAXALIGN(num_buckets * sizeof(slot_idx_t)) +
		   (Size) num_slots * sizeof(TsStatsChunk);
}

StaticAssertDecl(sizeof(slot_idx_t) == 2, "slot_idx_t is uint16; widen if you need >32768 slots");
