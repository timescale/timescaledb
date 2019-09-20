/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_COMPRESS_CHUNK_DML_H
#define TIMESCALEDB_COMPRESS_CHUNK_DML_H

#include <postgres.h>
#include <nodes/execnodes.h>
#include <foreign/fdwapi.h>

#include "hypertable.h"

typedef struct CompressChunkDmlPath
{
	CustomPath cpath;
	Oid chunk_relid;
} CompressChunkDmlPath;

typedef struct CompressChunkDmlState
{
	CustomScanState cscan_state;
	Oid chunk_relid;
} CompressChunkDmlState;

Path *compress_chunk_dml_generate_paths(Path *subpath, Chunk *chunk);

#define COMPRESS_CHUNK_DML_STATE_NAME "CompressChunkDmlState"
#endif
