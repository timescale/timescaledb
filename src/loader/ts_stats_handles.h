/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#pragma once

#include <compat/compat.h>
#include <postgres.h>
#include <storage/dsm.h>
#include <storage/lwlock.h>

/*
 * See more details in ts_stats_segment.c. These declarations are for the
 * registry of per-database DSM segments keyed by database OID. We cannot use
 * the DSM registry (GetNamedDSMSegment) for this, because it pins the named
 * segments forever and offers no way to remove them, so segments of dropped
 * databases would leak.
 */

#define TS_STATS_MAX_DATABASES 1024

/* Segment types */
#define TS_STATS_TYPE_CHUNK_SEGMENT 1

typedef struct TsObservHandleEntry
{
	Oid dboid;
	int segment_type;
	dsm_handle handle;
} TsObservHandleEntry;

typedef struct TsObservHandleTable
{
	LWLock lock;
	uint32 max_entries;
	TsObservHandleEntry entries[TS_STATS_MAX_DATABASES];
} TsObservHandleTable;

void ts_stats_shmem_request(void);
void ts_stats_shmem_startup(void);
