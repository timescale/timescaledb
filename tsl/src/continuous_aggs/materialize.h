/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_CONTINUOUS_AGGS_MATERIALIZE_H
#define TIMESCALEDB_TSL_CONTINUOUS_AGGS_MATERIALIZE_H

#include <postgres.h>
#include <fmgr.h>
#include <nodes/pg_list.h>

#include "continuous_aggs/common.h"
#include "ts_catalog/continuous_agg.h"

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
} InternalTimeRange;

InternalTimeRange continuous_agg_materialize_window_max(Oid timetype);
void continuous_agg_update_materialization(SchemaAndName partial_view,
										   SchemaAndName materialization_table,
										   const NameData *time_column_name,
										   InternalTimeRange new_materialization_range,
										   InternalTimeRange invalidation_range, int32 chunk_id);

void mattablecolumninfo_init(MatTableColumnInfo *matcolinfo, List *grouplist);
void mattablecolumninfo_addinternal(MatTableColumnInfo *matcolinfo, RangeTblEntry *usertbl_rte,
									int32 usertbl_htid);
int32 mattablecolumninfo_create_materialization_table(MatTableColumnInfo *matcolinfo,
													  int32 hypertable_id, RangeVar *mat_rel,
													  CAggTimebucketInfo *origquery_tblinfo,
													  bool create_addl_index, char *tablespacename,
													  char *table_access_method,
													  ObjectAddress *mataddress);
Query *mattablecolumninfo_get_partial_select_query(MatTableColumnInfo *matcolinfo,
												   Query *userview_query, bool finalized);

#endif /* TIMESCALEDB_TSL_CONTINUOUS_AGGS_MATERIALIZE_H */
