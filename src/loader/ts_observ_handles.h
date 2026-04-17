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
 * See more details in ts_observ_segment.c these declaration are only for the fallback
 * implementation of the per-database DSM segment for PG 15-16.
 */

#if PG17_LT
#define TS_OBSERV_MAX_DATABASES 128

typedef struct TsObservHandleEntry
{
	Oid dboid;
	dsm_handle handle;
} TsObservHandleEntry;

typedef struct TsObservHandleTable
{
	LWLock lock;
	uint32 max_entries;
	uint32 num_entries;
	TsObservHandleEntry entries[TS_OBSERV_MAX_DATABASES];
} TsObservHandleTable;

void ts_observ_shmem_request(void);
void ts_observ_shmem_startup(void);
#endif /* PG17_LT */
