/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TIMESCALEDB_CHUNK_DISPATCH_PLAN_H
#define TIMESCALEDB_CHUNK_DISPATCH_PLAN_H

#include <postgres.h>
#include <nodes/plannodes.h>
#include <nodes/parsenodes.h>
#include <nodes/extensible.h>

typedef struct ChunkDispatchPath
{
	CustomPath	cpath;
	ModifyTablePath *mtpath;
	Index		hypertable_rti;
	Oid			hypertable_relid;
} ChunkDispatchPath;

extern Path *ts_chunk_dispatch_path_create(ModifyTablePath *mtpath, Path *subpath, Index hypertable_rti, Oid hypertable_relid);

#endif							/* TIMESCALEDB_CHUNK_DISPATCH_PLAN_H */
