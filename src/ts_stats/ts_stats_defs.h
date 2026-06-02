/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <string.h>
#include <postgres.h>
#include "export.h"
#include <c.h>
#include <chunk_insert_state.h>
#include <datatype/timestamp.h>
#include <port/atomics.h>
#include <storage/lwlock.h>
#include <storage/shmem.h>

#include <sys/time.h>

#define TS_STATS_SHMEM_MAGIC 0x54534F42 /* "TSOB" — initialized-segment latch */

#define TS_STATS_BUCKETS 8
#define TS_STATS_PROBE_WIDTH 8
#define TS_STATS_CACHE_LINE_SIZE 64
#define TS_STATS_INVALID_IDX (-1)

/* note that the code expects TS_STATS_BUCKETS and TS_STATS_PROBE_WIDTH
 * to be small, < 256 numbers and uses uint8 at multiple places to store them.
 */
StaticAssertDecl(TS_STATS_BUCKETS < 256 && TS_STATS_PROBE_WIDTH < 256,
				 "TS_STATS_BUCKETS and TS_STATS_PROBE_WIDTH must be less than 256");

/*
 * Chunk information is stored in a shared-memory segment, one per database.
 *
 * The information is organized into two parallel arrays (they logically
 * belong together), one stores metadata about each chunk such as its ID and
 * a sequence number, and the other stores the actual statistics. The actual
 * stats are being read/written after the lookup on the metadata is done, so
 * the metadata is held together for better cache locality.
 *
 * The table is organised into TS_STATS_BUCKETS buckets, and we keep track of
 * the last update sequence number for these buckets. In each bucket the chunk id
 * (compressed_relid) is hashed to find the 'home location' within the bucket.
 * If the bucket is occupied by another chunk, we do a linear probe of up to
 * TS_STATS_PROBE_WIDTH slots within the bucket. If the chunk is not found
 * we proceed to the next bucket. We traverse the buckets in recency order.
 * If we still can't find the chunk after traversing all buckets, it doesn't
 * exist. On upsert, we check the entries in the last (the oldest bucket)
 * within the search window (TS_STATS_PROBE_WIDTH slots) for eviction
 * candidates. If we find an empty slot, we use it. If not we evict the oldest
 * entry in the search window of the last bucket.
 *
 * Once we updated an entry in a bucket we update the last update sequence
 * number for the bucket, so the next upsert will traverse the buckets in
 * the order of recency.
 */

typedef struct TsStatsRelids
{
	Oid compressed_relid;
	Oid uncompressed_relid;
} TsStatsRelids;

/*
 * As explained above, TsStatsChunkMetadata is split off from TsStatsChunk
 * because these are needed more frequently than the rest of the structure,
 * because during traversal we read these fields first to find the entry.
 * Keeping them together allows better cache locality for the common case.
 *
 * The 'state' field is composed of two parts: the lower two bits are flags,
 * and the rest is a version counter. The valid flag is set when the slot is
 * initialized with valid data, and being cleared at eviction. The version
 * counter is incremented at every update, and readers can use it to order
 * updates.
 */
typedef struct TsStatsChunkMetadata
{
	/* state layout:
	 * bit 0: is valid (valid when set)
	 * bit 1: in progress (being updated, not safe to read or evict)
	 * bits 2-63: seqno, incremented at every update for recency tracking
	 */
	pg_atomic_uint64 state;
	/* Chunks are identified by relids.compressed_relid */
	TsStatsRelids relids;
} TsStatsChunkMetadata;

/* Bit flag values as per the above: */
#define TS_STATS_CHUNK_METADATA_VALID_FLAG ((uint64) 1)
#define TS_STATS_CHUNK_METADATA_IN_PROGRESS_FLAG ((uint64) 2)
/* Bit mask helpers */
#define TS_STATS_CHUNK_METADATA_SEQNO_SHIFT 2
#define TS_STATS_CHUNK_METADATA_SEQNO_MASK (~((1ULL << TS_STATS_CHUNK_METADATA_SEQNO_SHIFT) - 1))
#define TS_STATS_CHUNK_METADATA_GET_SEQNO(state)                                                   \
	((state & TS_STATS_CHUNK_METADATA_SEQNO_MASK) >> TS_STATS_CHUNK_METADATA_SEQNO_SHIFT)
#define TS_STATS_CHUNK_METADATA_IS_VALID(state) ((state & TS_STATS_CHUNK_METADATA_VALID_FLAG) != 0)
#define TS_STATS_CHUNK_METADATA_IS_IN_PROGRESS(state)                                              \
	((state & TS_STATS_CHUNK_METADATA_IN_PROGRESS_FLAG) != 0)

/*
 * MinMaxSumDataWA uses atomics, but updates to this whole structure are not atomic.
 * WA means 'With Atomics'. The potential inconsistency is acceptable for our use
 * case, and it allows us to update the stats without acquiring an exclusive
 * lock, which reduces contention and improves performance.
 */
typedef struct MinMaxSumDataWA
{
	/*
	 * The sqsum_val is a sum of squares, which is calculated as
	 * a double value and then cast to uint64 for atomic operations.
	 */
	pg_atomic_uint64 sqsum_val;
	pg_atomic_uint64 sum_val;
	pg_atomic_uint32 min_val;
	pg_atomic_uint32 max_val;
} MinMaxSumDataWA;

typedef struct MinMaxSumData
{
	double sqsum_val;
	uint64 sum_val;
	uint32 min_val;
	uint32 max_val;
} MinMaxSumData;

/*
 * SharedCountersWA uses atomics, and mirrors the structure of SharedCounters.
 * Updates to this structure are not atomic, WA means 'With Atomics'. The
 * potential inconsistency is acceptable for our use case, and it allows us to
 * update the stats without acquiring an exclusive lock.
 */
typedef struct SharedCountersWA
{
	pg_atomic_uint64 batches_deleted;
	pg_atomic_uint64 batches_decompressed;
	pg_atomic_uint64 tuples_decompressed;
	pg_atomic_uint64 batches_scanned;
	pg_atomic_uint64 batches_checked_by_bloom;
	pg_atomic_uint64 batches_pruned_by_bloom;
	pg_atomic_uint64 batches_without_bloom;
	pg_atomic_uint64 batches_bloom_false_positives;
	pg_atomic_uint64 batches_filtered_compressed;
	pg_atomic_uint64 batches_filtered_decompressed;
} SharedCountersWA;

typedef struct TsStatsChunk
{
	/*
	 * Microseconds since segment->base_timestamp. These are
	 * not used in CAS, only EQ and Assign, we can afford
	 * these to be non atomic.
	 */
	uint64 first_update_us;
	uint64 last_update_us;

	/* Chunk stats at compression, totals: */
	pg_atomic_uint32 compressed_batch_count;
	pg_atomic_uint32 compressed_block_count;

	/* byte stat across all batches and columns: */
	MinMaxSumDataWA compressed_block_bytes;

	/* sum bytes for each column, across batches: */
	MinMaxSumDataWA compressed_batch_bytes;

	/* rows across batches: */
	MinMaxSumDataWA compressed_batch_rows;

	/* Aggregated Cmd stats */
	SharedCountersWA cmd_totals;

	/* Cmd stats for the last operation */
	SharedCountersWA cmd_last_op;

	/* Operation counts */
	pg_atomic_uint32 n_selects;
	pg_atomic_uint32 n_inserts;
	pg_atomic_uint32 n_updates;
	pg_atomic_uint32 n_deletes;

} TsStatsChunk;

typedef struct TsStatsChunkBucketMetadata
{
	pg_atomic_uint64 last_update_seqno;
	/* padding to avoid false sharing on 64 byte cache lines */
	uint8 padding[56];
} TsStatsChunkBucketMetadata;

typedef struct TsStatsChunkSegment
{
	/* Magic set once at init, never written again*/
	uint32 magic; /* TS_STATS_SHMEM_MAGIC when initialized */
	Oid dboid;	  /* database that owns this segment */

	/* Only used during whole segment operations, such as initialization. */
	LWLock lock;

	/* base timestamp for *_update_us fields */
	TimestampTz base_timestamp;

	/* Number of slots */
	uint32 num_slots;

	/* Sequence number shared across all chunk slots */
	pg_atomic_uint64 update_seqno;

	/*
	 * Trailing arrays — dynamically sized and aligned to cache lines.
	 *
	 * The logical layout is:
	 *
	 *   TsStatsChunkMetadata        meta[num_slots];
	 *   TsStatsChunk                slots[num_slots];
	 *   TsStatsChunkBucketMetadata  buckets[TS_STATS_BUCKETS];
	 *
	 */
} TsStatsChunkSegment;

#define TS_STATS_CACHE_PADDED(S) (TYPEALIGN((TS_STATS_CACHE_LINE_SIZE), (S)))

static inline Size
ts_stats_chunk_data_size(uint32 num_slots)
{
	Size size = 0;
	size += TS_STATS_CACHE_PADDED(num_slots * sizeof(TsStatsChunkMetadata));
	size += TS_STATS_CACHE_PADDED(num_slots * sizeof(TsStatsChunk));
	size += TS_STATS_CACHE_PADDED(TS_STATS_BUCKETS * sizeof(TsStatsChunkBucketMetadata));
	return size;
}

static inline Size
ts_stats_chunk_segment_size(uint32 num_slots)
{
	Size size = TS_STATS_CACHE_PADDED(sizeof(TsStatsChunkSegment));
	return add_size(size, ts_stats_chunk_data_size(num_slots));
}

static inline TsStatsChunkMetadata *
ts_stats_chunk_metadata(TsStatsChunkSegment *seg)
{
	return (TsStatsChunkMetadata *) ((char *) seg +
									 TS_STATS_CACHE_PADDED(sizeof(TsStatsChunkSegment)));
}

static inline TsStatsChunk *
ts_stats_chunk_slots(TsStatsChunkSegment *seg)
{
	return (TsStatsChunk *) ((char *) seg + TS_STATS_CACHE_PADDED(sizeof(TsStatsChunkSegment)) +
							 TS_STATS_CACHE_PADDED(seg->num_slots * sizeof(TsStatsChunkMetadata)));
}

static inline TsStatsChunkBucketMetadata *
ts_stats_chunk_bucket_metadata(TsStatsChunkSegment *seg)
{
	return (TsStatsChunkBucketMetadata *) ((char *) seg +
										   TS_STATS_CACHE_PADDED(sizeof(TsStatsChunkSegment)) +
										   TS_STATS_CACHE_PADDED(seg->num_slots *
																 sizeof(TsStatsChunkMetadata)) +
										   TS_STATS_CACHE_PADDED(seg->num_slots *
																 sizeof(TsStatsChunk)));
}

#undef TS_STATS_CACHE_PADDED

static inline void
ts_stats_chunk_init_slot(TsStatsChunk *slot)
{
	/* memset all to zero, except the min values */
	memset(slot, 0, sizeof(TsStatsChunk));
	pg_atomic_write_u32(&slot->compressed_block_bytes.min_val, UINT32_MAX);
	pg_atomic_write_u32(&slot->compressed_batch_bytes.min_val, UINT32_MAX);
	pg_atomic_write_u32(&slot->compressed_batch_rows.min_val, UINT32_MAX);
}

static inline void
init_min_max_sum(MinMaxSumData *mmsd)
{
	mmsd->sqsum_val = 0;
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

	double square_value = (double) value * (double) value;
	m->sqsum_val += square_value;
}

#define TS_STATS_MMS_CAS_MAX_RETRIES 5

static inline void
mms_merge(MinMaxSumDataWA *dst, const MinMaxSumData *src)
{
	/*
	 * this function does a bounded CAS loop so it won't spin indefinitely and does a best
	 * effort update of the min, max, sum, and sqsum values.
	 */
	uint32 cur_min = pg_atomic_read_u32(&dst->min_val);
	for (int r = 0; src->min_val < cur_min && r < TS_STATS_MMS_CAS_MAX_RETRIES; r++)
	{
		if (pg_atomic_compare_exchange_u32(&dst->min_val, &cur_min, src->min_val))
		{
			break;
		}
		/* CAS failed, stay with the old value */
	}

	uint32 cur_max = pg_atomic_read_u32(&dst->max_val);
	for (int r = 0; src->max_val > cur_max && r < TS_STATS_MMS_CAS_MAX_RETRIES; r++)
	{
		if (pg_atomic_compare_exchange_u32(&dst->max_val, &cur_max, src->max_val))
		{
			break;
		}
		/* CAS failed, stay with the old value */
	}

	pg_atomic_add_fetch_u64(&dst->sum_val, src->sum_val);

	uint64 cur_sqsum = pg_atomic_read_u64(&dst->sqsum_val);
	for (int r = 0; r < TS_STATS_MMS_CAS_MAX_RETRIES; r++)
	{
		double new_d;
		memcpy(&new_d, &cur_sqsum, sizeof(double));
		new_d += src->sqsum_val;

		uint64 desired;
		memcpy(&desired, &new_d, sizeof(uint64));

		if (pg_atomic_compare_exchange_u64(&dst->sqsum_val, &cur_sqsum, desired))
		{
			break;
		}
		/* CAS failed, stay with the old value */
	}
}

#undef TS_STATS_MMS_CAS_MAX_RETRIES

static inline void
mms_update(MinMaxSumDataWA *dst, const MinMaxSumData *src)
{
	pg_atomic_write_u32(&dst->min_val, src->min_val);
	pg_atomic_write_u32(&dst->max_val, src->max_val);
	pg_atomic_write_u64(&dst->sum_val, src->sum_val);

	/* sqsum is calculated as a double and then copied to uint64 for atomic operations */
	pg_atomic_write_u64(&dst->sqsum_val, *((uint64 *) &src->sqsum_val));
}

static inline double
mms_sqsum(MinMaxSumDataWA *m)
{
	uint64 sqsum_val = pg_atomic_read_u64(&m->sqsum_val);
	return *((double *) &sqsum_val);
}
