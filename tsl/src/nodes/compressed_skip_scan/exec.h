/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifndef TIMESCALEDB_COMPRESSED_SKIP_SCAN_EXEC_H
#define TIMESCALEDB_COMPRESSED_SKIP_SCAN_EXEC_H

#include <postgres.h>
#include "nodes/nodes_common.h"

typedef struct CompressedChunkColumnState
{
	CompressedChunkColumnType type;
	Oid typid;

	/*
	 * Attno of the decompressed column in the output of DecompressChunk node.
	 * Negative values are special columns that do not have a representation in
	 * the decompressed chunk, but are still used for decompression. They should
	 * have the respective `type` field.
	 */
	AttrNumber output_attno;

	/*
	 * Attno of the compressed column in the input compressed chunk scan.
	 */
	AttrNumber compressed_scan_attno;

	union
	{
		struct
		{
			Datum value;
			bool isnull;
			int count;
		} segmentby;

		struct
		{
			/* For row-by-row decompression. */
			DecompressionIterator *iterator;

		} compressed;
	};
} CompressedChunkColumnState;

typedef struct CompressedSkipScanState
{
	CustomScanState csstate;
	List *decompression_map;
	int num_columns;
	CompressedChunkColumnState *columns;
	bool initialized;
	bool reverse;
	int hypertable_id;
	Oid chunk_relid;
	List *hypertable_compression_info;
	int counter;
	MemoryContext per_batch_context;
} CompressedSkipScanState;

extern Node *compressed_skip_scan_state_create(CustomScan *cscan);

#endif /* TIMESCALEDB_COMPRESSED_SKIP_SCAN_EXEC_H */
