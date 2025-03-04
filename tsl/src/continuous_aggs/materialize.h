/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include <fmgr.h>
#include <nodes/pg_list.h>

#include "common.h"
#include "ts_catalog/continuous_agg.h"

typedef struct SchemaAndName
{
	Name schema;
	Name name;
} SchemaAndName;

void continuous_agg_update_materialization(Hypertable *mat_ht, const ContinuousAgg *cagg,
										   SchemaAndName partial_view,
										   SchemaAndName materialization_table,
										   const NameData *time_column_name,
										   InternalTimeRange new_materialization_range,
										   InternalTimeRange invalidation_range, int32 chunk_id);
