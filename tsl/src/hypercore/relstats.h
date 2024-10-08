/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

typedef struct RelStats
{
	float4 reltuples;
	int32 relpages;
	int32 relallvisible;
} RelStats;

extern void relstats_fetch(Oid relid, RelStats *stats);
extern void relstats_update(Oid relid, const RelStats *stats);
