/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/*
 * Unit tests for the chunk-stats for the parts that are hard to
 * reach from SQL.
 *
 * The DSM/shmem layer is bypassed: we palloc a tiny segment in regular
 * backend memory, set its structural fields.
 */

#include <postgres.h>
#include <fmgr.h>
#include <math.h>

#include "test_utils.h"

#include "ts_stats/ts_stats_defs.h"
#include "ts_stats/ts_stats_segment.h"

static TsStatsChunkSegment *
make_test_segment(uint32 num_slots)
{
	TsStatsChunkSegment *seg = palloc0(ts_stats_chunk_segment_size(num_slots));
	seg->magic = TS_STATS_SHMEM_MAGIC;
	seg->dboid = MyDatabaseId;
	seg->base_timestamp = GetCurrentTimestamp();
	seg->num_slots = num_slots;
	return seg;
}

static void
test_simple_upsert_lookup()
{
	TsStatsChunkSegment *seg = make_test_segment(16);
	TsStatsRelids relids = {
		.compressed_relid = 12345,
		.uncompressed_relid = 54321,
	};
	int32 slot_idx = ts_stats_chunk_segment_prepare_upsert(seg, relids);
	Assert(slot_idx != TS_STATS_INVALID_IDX && "Failed to prepare upsert");
	ts_stats_chunk_segment_finish_upsert(seg, slot_idx, relids.compressed_relid);
	Assert(ts_stats_chunk_segment_lookup(seg, relids.compressed_relid) == slot_idx &&
		   "Lookup index does not match upsert index");
	pfree(seg);
}

static int32
insert_one_chunk(TsStatsChunkSegment *seg, TsStatsRelids relids)
{
	int32 slot_idx = ts_stats_chunk_segment_prepare_upsert(seg, relids);
	Assert(slot_idx != TS_STATS_INVALID_IDX && "Failed to prepare upsert");
	ts_stats_chunk_segment_finish_upsert(seg, slot_idx, relids.compressed_relid);
	return slot_idx;
}

static void
test_eviction()
{
#define NUM_SLOTS 1024
#define MIN_LIFESPAN (NUM_SLOTS / TS_STATS_PROBE_WIDTH / TS_STATS_BUCKETS)
	TsStatsChunkSegment *seg = make_test_segment(NUM_SLOTS);
	TsStatsChunkMetadata *meta_base = ts_stats_chunk_metadata(seg);
	Oid base_compressed_relid = 10000;
	Oid uncompressed_relid = 54321;
	int32 slot_indices[NUM_SLOTS] = { 0 };
	uint64 slot_seqno[NUM_SLOTS] = { 0 };
	double sum_lifespan = 0;
	double sumsq_lifespan = 0;
	int32 num_evictions = 0;
	uint64 min_lifespan = UINT64_MAX;
	uint64 max_lifespan = 0;

	for (int j = 0; j < 10; j++)
	{
		for (int i = 0; i < NUM_SLOTS; i++)
		{
			/*
			 * the i,j makes sure we don't always hit the same slots which would favour the
			 * eviction policy.
			 */
			TsStatsRelids relids = {
				.compressed_relid = base_compressed_relid + i + (j * 11 * NUM_SLOTS),
				.uncompressed_relid = uncompressed_relid,
			};
			int32 slot_idx = insert_one_chunk(seg, relids);
			Assert(slot_idx != TS_STATS_INVALID_IDX && "Failed to prepare upsert");
			Assert(slot_idx < NUM_SLOTS &&
				   "Slot index exceeds number of slots, eviction should have happened");
			if (slot_indices[slot_idx] != 0)
			{
				uint64 old_seqno = slot_seqno[slot_idx];
				uint64 new_seqno = pg_atomic_read_u64(&meta_base[slot_idx].state) >>
								   TS_STATS_CHUNK_METADATA_SEQNO_SHIFT;
				uint64 lifespan = new_seqno - old_seqno;
				sum_lifespan += lifespan;
				sumsq_lifespan += (double) lifespan * lifespan;
				num_evictions++;
				if (lifespan < min_lifespan)
				{
					min_lifespan = lifespan;
				}
				if (lifespan > max_lifespan)
				{
					max_lifespan = lifespan;
				}
				Assert(new_seqno > (old_seqno + MIN_LIFESPAN) &&
					   "Sequence number did not increase as expected after eviction");
			}
			slot_indices[slot_idx] = relids.compressed_relid;
			slot_seqno[slot_idx] = pg_atomic_read_u64(&meta_base[slot_idx].state) >>
								   TS_STATS_CHUNK_METADATA_SEQNO_SHIFT;
			Assert(ts_stats_chunk_segment_lookup(seg, relids.compressed_relid) == slot_idx &&
				   "Lookup index does not match upsert index");
		}
	}

	elog(NOTICE,
		 "Eviction test completed: %d evictions, %d slots, average lifespan %.0f, lifespan stddev "
		 "%.0f, min lifespan %d, max lifespan %d",
		 num_evictions,
		 NUM_SLOTS,
		 (double) sum_lifespan / num_evictions,
		 sqrt((sumsq_lifespan / num_evictions) -
			  ((sum_lifespan / num_evictions) * (sum_lifespan / num_evictions))),
		 (int) min_lifespan,
		 (int) max_lifespan);
	pfree(seg);
}

TS_TEST_FN(ts_test_chunk_stats)
{
	test_simple_upsert_lookup();
	test_eviction();
	PG_RETURN_VOID();
}
