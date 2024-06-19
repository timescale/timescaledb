/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include "batch_queue.h"
#include "decompress_context.h"
#include <nodes/extensible.h>

#define DECOMPRESS_CHUNK_COUNT_ID -9
#define DECOMPRESS_CHUNK_SEQUENCE_NUM_ID -10

typedef struct DecompressChunkState
{
	CustomScanState csstate;
	List *decompression_map;
	List *is_segmentby_column;
	List *bulk_decompression_column;
	List *custom_scan_tlist;
	bool has_row_marks;

	DecompressContext decompress_context;

	int hypertable_id;
	Oid chunk_relid;

	BatchQueue *batch_queue;
	CustomExecMethods exec_methods;

	List *sortinfo;

	/*
	 * For some predicates, we have more efficient implementation that work on
	 * the entire compressed batch in one go. They go to this list, and the rest
	 * goes into the usual ss.ps.qual. Note that we constify stable functions
	 * in these predicates at execution time, but have to keep the original
	 * version for EXPLAIN. We also need special handling for quals that
	 * evaluate to constant false, hence the flag.
	 */
	List *vectorized_quals_original;
} DecompressChunkState;

extern Node *decompress_chunk_state_create(CustomScan *cscan);

TupleTableSlot *decompress_chunk_exec_vector_agg_impl(CustomScanState *vector_agg_state,
													  DecompressChunkState *decompress_state);
