/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <nodes/plannodes.h>
#include <utils/relcache.h>

#include "nodes/decompress_chunk/vector_quals.h"

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

/*
 * VectorAggPlanInfo.
 *
 * State and interface for implementing planning of VectorAgg over different
 * child nodes.
 */
typedef struct VectorAggPlanInfo
{
	/* Child plan over which VectorAgg is being planned */
	Scan *childplan;

	/*
	 * Check if a var references a vectorizable column, and, is optionally a
	 * segmentby column.
	 */
	bool (*is_vector_var)(struct VectorAggPlanInfo *vpinfo, const Var *var, bool *is_segmentby);

	/*
	 * Build supplementary info to determine whether we can vectorize the
	 * aggregate FILTER clauses.
	 */
	VectorQualInfo (*build_aggfilter)(struct VectorAggPlanInfo *vpinfo);
} VectorAggPlanInfo;

extern void _vector_agg_init(void);
extern bool vectoragg_plan_info_init_decompress_chunk(struct VectorAggPlanInfo *vpinfo,
													  Relation rel);
Plan *try_insert_vector_agg_node(Plan *plan, List *rtable);
bool has_vector_agg_node(Plan *plan, bool *has_normal_agg);
