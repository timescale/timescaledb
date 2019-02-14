/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_CHUNK_DISPATCH_PLAN_H
#define TIMESCALEDB_CHUNK_DISPATCH_PLAN_H

#include <postgres.h>
#include <nodes/plannodes.h>
#include <nodes/parsenodes.h>
#include <nodes/extensible.h>

#include "export.h"

typedef struct ChunkDispatchPath
{
	CustomPath cpath;
	ModifyTablePath *mtpath;
	Index hypertable_rti;
	Oid hypertable_relid;
} ChunkDispatchPath;

extern TSDLLEXPORT Path *ts_chunk_dispatch_path_create(PlannerInfo *root, ModifyTablePath *mtpath,
													   Index hypertable_rti, int subpath_index);

#endif /* TIMESCALEDB_CHUNK_DISPATCH_PLAN_H */
