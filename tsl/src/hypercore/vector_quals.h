/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <nodes/execnodes.h>

#include "nodes/decompress_chunk/vector_quals.h"

extern void vector_qual_state_init(VectorQualState *vqstate, List *quals, TupleTableSlot *slot);
extern void vector_qual_state_reset(VectorQualState *vqstate);
extern const ArrowArray *vector_qual_state_get_arrow_array(VectorQualState *vqstate, Expr *expr,
														   bool *is_default_value);
extern uint16 ExecVectorQual(VectorQualState *vqstate, ExprContext *econtext);
