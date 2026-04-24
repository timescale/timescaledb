/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include "common.h"
#include "ts_catalog/continuous_agg.h"
#include <fmgr.h>
#include <nodes/pg_list.h>

typedef struct SchemaAndName
{
	Name schema;
	Name name;
} SchemaAndName;

/***********************
 * Time ranges
 ***********************/

typedef struct TimeRange
{
	Oid type;
	Datum start;
	Datum end;
} TimeRange;

typedef struct InternalTimeRange
{
	Oid type;
	int64 start; /* inclusive */
	int64 end;	 /* exclusive */
	bool start_isnull;
	bool end_isnull;
} InternalTimeRange;

void continuous_agg_update_materialization(Hypertable *mat_ht, const ContinuousAgg *cagg,
										   SchemaAndName partial_view,
										   SchemaAndName materialization_table,
										   const NameData *time_column_name,
										   InternalTimeRange materialization_range);

/*
 * tenant_values_array must be a constructed ArrayType* (as Datum) whose element
 * type is tenant_type. The SQL uses `tenant_col = ANY($3)`, so a one-element
 * array is the normal form for refreshing a single tenant.
 */
void continuous_agg_update_materialization_for_tenant(Hypertable *mat_ht, const ContinuousAgg *cagg,
													  SchemaAndName partial_view,
													  SchemaAndName materialization_table,
													  const NameData *time_column_name,
													  InternalTimeRange materialization_range,
													  const NameData *tenant_column_name,
													  Datum tenant_values_array, Oid tenant_type);
