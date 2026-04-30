/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include "ts_observ_defs.h"
#include <datatype/timestamp.h>
#include <storage/lwlock.h>

#define TS_OBSERV_SHMEM_MAGIC 0x54535354 /* "TSST" — marks segment as initialized */

typedef struct TsObservLog
{
	uint32 capacity;
	uint32 capacity_mask;
	/* TsObservTuple entries[capacity] in trailing memory */
} TsObservLog;

typedef struct TsObservSegment
{
	/* TS_OBSERV_SHMEM_MAGIC when initialized, 0 otherwise */
	uint32 magic;
	LWLock lock;
	pg_atomic_uint64 next_seqno;
	pg_atomic_uint64 last_event_ts;
	TimestampTz base_timestamp;
	Oid dboid;
	TsObservLog log;
	/* Followed by: TsObservTuple entries[capacity] */
} TsObservSegment;

/* Access the segment for the current database. NULL if unavailable. */
extern TSDLLEXPORT TsObservSegment *ts_observ_get_segment(void);

/* Compute total segment size from GUC capacity. */
extern TSDLLEXPORT Size ts_observ_segment_size(void);

/* Helper to access a specific log entry. */
static inline TsObservTuple *
ts_observ_log_entry(TsObservSegment *seg, uint32 pos)
{
	char *base = (char *) seg + MAXALIGN(sizeof(TsObservSegment));
	return (TsObservTuple *) (base + pos * sizeof(TsObservTuple));
}
