/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifndef TIMESCALEDB_DECOMPRESS_CHUNK_EXEC_H
#define TIMESCALEDB_DECOMPRESS_CHUNK_EXEC_H

#include <postgres.h>

#include <nodes/extensible.h>
#include "batch_queue.h"
#include "decompress_context.h"

#define DECOMPRESS_CHUNK_COUNT_ID -9
#define DECOMPRESS_CHUNK_SEQUENCE_NUM_ID -10

typedef struct DecompressChunkState
{
	CustomScanState csstate;
	List *decompression_map;
	List *is_segmentby_column;
	List *bulk_decompression_column;
	List *aggregated_column_type;
	List *custom_scan_tlist;

	DecompressContext decompress_context;

	int hypertable_id;
	Oid chunk_relid;

	BatchQueue *batch_queue;
	CustomExecMethods exec_methods;

	List *sortinfo;

	/* Perform calculation of the aggregate directly in the decompress chunk node and emit partials
	 */
	bool perform_vectorized_aggregation;

	/*
	 * For some predicates, we have more efficient implementation that work on
	 * the entire compressed batch in one go. They go to this list, and the rest
	 * goes into the usual ss.ps.qual. Note that we constify stable functions
	 * in these predicates at execution time, but have to keep the original
	 * version for EXPLAIN. We also need special handling for quals that
	 * evaluate to constant false, hence the flag.
	 */
	List *vectorized_quals_original;
	bool have_constant_false_vectorized_qual;
} DecompressChunkState;

extern Node *decompress_chunk_state_create(CustomScan *cscan);

#endif /* TIMESCALEDB_DECOMPRESS_CHUNK_EXEC_H */
