/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_FROZEN_CHUNK_DML_H
#define TIMESCALEDB_FROZEN_CHUNK_DML_H

#include <postgres.h>
#include <nodes/execnodes.h>

#include "hypertable.h"

typedef struct FrozenChunkDmlPath
{
	CustomPath cpath;
	Oid chunk_relid;
} FrozenChunkDmlPath;

typedef struct FrozenChunkDmlState
{
	CustomScanState cscan_state;
	Oid chunk_relid;
} FrozenChunkDmlState;

Path *frozen_chunk_dml_generate_path(Path *subpath, Chunk *chunk);

#define FROZEN_CHUNK_DML_STATE_NAME "FrozenChunkDmlState"
#endif
