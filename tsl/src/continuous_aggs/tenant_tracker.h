/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <utils/timestamp.h>

/*
 * Maximum exact-storable tenant key length.  Exposed because callers
 * validate key length before handing keys to the write path; longer
 * keys force the generation INVALID.
 */
#define TENANT_TRACKER_KEY_MAXLEN 64
typedef struct TenantTracking TenantTracking;
typedef struct TenantGeneration TenantGeneration;

/*
 * Read-only snapshot of a tracker's current state, Not
 * synchronized against concurrent writers/flush -- a best-effort read.
 */
typedef struct TenantTrackerInfo
{
	int32 seqnum;
	uint32 active_generation;
	uint32 nentries;
	uint32 status;
	int64 late_threshold_start;
	int64 late_threshold_end;
} TenantTrackerInfo;

extern TenantTracking *ts_tenant_tracker_get_or_attach(int32 hypertable_id,
													   int64 late_threshold_start,
													   int64 late_threshold_end);

extern bool ts_tenant_tracker_update(TenantTracking *tracking, const char *key, uint16 key_len,
									 TimestampTz min_ts, TimestampTz max_ts);

/*
 * Streaming batch drain: pin the active generation ONCE, apply many tenants
 * (no intermediate array or copy), then unpin.  Always pair begin with end.
 * returned handle is pinned generation; treat it as opaque and pass it to
 * apply_one/end.
 */
extern TenantGeneration *ts_tenant_tracker_begin_batch(TenantTracking *tracking, int32 *seqnum,
													   int64 *late_threshold_start,
													   int64 *late_threshold_end);
/* Returns false once the generation is INVALID (full / unstorable key); the
 * caller should then stop draining -- nothing more can be recorded. */
extern bool ts_tenant_tracker_apply_one(TenantGeneration *generation, const char *key,
										uint16 key_len, TimestampTz min_ts, TimestampTz max_ts);
extern void ts_tenant_tracker_end_batch(TenantGeneration *generation);

/*
 * Force the active generation INVALID (e.g. a tenant key could not be encoded,
 */
extern void ts_tenant_tracker_mark_invalid(TenantTracking *tracking);

extern TenantTracking *ts_tenant_tracker_lookup(int32 hypertable_id);

extern void ts_tenant_tracker_get_info(TenantTracking *tracking, TenantTrackerInfo *info);

extern void ts_tenant_tracker_flush(TenantTracking *tracking, int32 hypertable_id,
									int64 late_threshold_start, int64 late_threshold_end);
