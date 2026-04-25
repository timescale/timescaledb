/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include "ts_observ_write.h"
#include "ts_observ_segment.h"

#include <postgres.h>
#include <miscadmin.h>
#include <storage/lwlock.h>
#include <utils/timestamp.h>

/*
 * Claim a unique virtual timestamp for a new event. Multiple tuples written
 * with the same VTS form one logical event.
 */
static uint64
ts_observ_claim_event_ts(TsObservSegment *seg)
{
	TimestampTz now = GetCurrentTimestamp();
	uint64 us_offset = (uint64) (now - seg->base_timestamp);
	uint64 virtual_ts = us_offset << TS_OBSERV_SUB_US_BITS;

	while (true)
	{
		uint64 last = pg_atomic_read_u64(&seg->last_event_ts);
		uint64 candidate = Max(virtual_ts, last + 1);
		if (pg_atomic_compare_exchange_u64(&seg->last_event_ts, &last, candidate))
			return candidate;
	}
}

void
ts_observ_emit(const TsObservKV *kvs, int count)
{
	TsObservSegment *seg = ts_observ_get_segment();
	if (seg == NULL)
		return;

	uint64 vts = ts_observ_claim_event_ts(seg);

	/* Reserve `count` consecutive slots atomically. */
	uint64 base_seqno = pg_atomic_fetch_add_u64(&seg->next_seqno, count);

	LWLockAcquire(&seg->lock, LW_EXCLUSIVE);
	for (int i = 0; i < count; i++)
	{
		uint32 pos = (uint32) ((base_seqno + i) & seg->log.capacity_mask);
		*ts_observ_log_entry(seg, pos) = (TsObservTuple){
			.id = ts_observ_make_id(vts, kvs[i].key_id),
			.value = kvs[i].value,
		};
	}
	LWLockRelease(&seg->lock);
}
