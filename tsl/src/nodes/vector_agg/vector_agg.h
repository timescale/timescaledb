/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>

#include <nodes/plannodes.h>
#include <nodes/execnodes.h>

typedef struct VectorAggState
{
	CustomScanState custom;
} VectorAggState;

typedef struct VectorAggPlan
{
	CustomScan custom;
} VectorAggPlan;

extern Plan *vector_agg_plan_create(Agg *agg, CustomScan *decompress_chunk);

extern Node *vector_agg_state_create(CustomScan *cscan);

extern void _vector_agg_init(void);
