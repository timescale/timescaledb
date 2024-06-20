/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <executor/tuptable.h>
#include <nodes/primnodes.h>

#include "vector_predicates.h"
#include <compression/arrow_c_data_interface.h>

/*
 * VectorQualInfo provides planner time information for extracting
 * vectorizable quals from regular quals.
 */
typedef struct VectorQualInfo
{
	/* The range-table index of the relation to compute vectorized quals
	 * for */
	Index rti;

	/* AttrNumber-indexed array indicating whether an attribute/column is a
	 * vectorizable type */
	bool *vector_attrs;
} VectorQualInfo;

/*
 * VectorQualState keeps the necessary state needed for the computation of
 * vectorized filters in scan nodes.
 *
 * As such, it provides a common interface for scan nodes, e.g., ColumnarScan
 * and DecompressChunk.
 */
typedef struct VectorQualState
{
	List *vectorized_quals_constified;
	uint16 num_results;
	uint64 *vector_qual_result;
	MemoryContext per_vector_mcxt;
	TupleTableSlot *slot;

	/*
	 * Interface function to be provided by scan node.
	 *
	 * Given a (compressed) tuple/slot, and a column reference (Var), get the
	 * corresponding arrow array.
	 *
	 * Scan-node specific context data can be provided by wrapping this struct
	 * in a larger one.
	 */
	const ArrowArray *(*get_arrow_array)(struct VectorQualState *vqstate, const Var *var,
										 bool *is_default_value);
} VectorQualState;

extern Node *vector_qual_make(Node *qual, const VectorQualInfo *vqinfo);
extern VectorQualSummary vector_qual_compute(VectorQualState *vqstate);
extern ArrowArray *make_single_value_arrow(Oid pgtype, Datum datum, bool isnull);
