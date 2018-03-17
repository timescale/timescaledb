#ifndef TIMESCALEDB_SPACE_PARTITION_EXCLUDE_H
#define TIMESCALEDB_SPACE_PARTITION_EXCLUDE_H

#include <postgres.h>
#include "dimension.h"

typedef struct HypertableRestrictInfo
{
	Dimension  *dim;
	int32		value;
} HypertableRestrictInfo;

HypertableRestrictInfo *hypertable_restrict_info_get(RestrictInfo *base_restrict_info, Query *query, Hypertable *hentry);

void hypertable_restrict_info_apply(HypertableRestrictInfo *spe,
							   PlannerInfo *root,
							   Hypertable *ht,
							   Index main_table_child_index);


#endif							/* TIMESCALEDB_SPACE_PARTITION_EXCLUDE_H */
