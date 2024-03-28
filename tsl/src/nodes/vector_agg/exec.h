/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>

#include <nodes/execnodes.h>

typedef struct VectorAggState
{
	CustomScanState custom;
} VectorAggState;

extern Node *vector_agg_state_create(CustomScan *cscan);
