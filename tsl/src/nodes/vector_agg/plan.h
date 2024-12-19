/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>

#include <nodes/plannodes.h>

typedef struct VectorAggPlan
{
	CustomScan custom;
} VectorAggPlan;

/*
 * The indexes of settings that we have to pass through the custom_private list.
 */
typedef enum
{
	VASI_GroupingType = 0,
	VASI_Count
} VectorAggSettingsIndex;

extern void _vector_agg_init(void);

Plan *try_insert_vector_agg_node(Plan *plan);
bool has_vector_agg_node(Plan *plan, bool *has_normal_agg);
