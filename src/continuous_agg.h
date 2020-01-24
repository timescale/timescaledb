/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#ifndef TIMESCALEDB_CONTINUOUS_AGG_H
#define TIMESCALEDB_CONTINUOUS_AGG_H
#include <postgres.h>
#include <catalog/pg_type.h>

#include <catalog.h>
#include <chunk.h>

#include "with_clause_parser.h"
#include "compat.h"

#define CAGGINVAL_TRIGGER_NAME "ts_cagg_invalidation_trigger"

typedef enum ContinuousAggViewOption
{
	ContinuousEnabled = 0,
	ContinuousViewOptionRefreshLag,
	ContinuousViewOptionRefreshInterval,
	ContinuousViewOptionMaxIntervalPerRun,
	ContinuousViewOptionCreateGroupIndex,
	ContinuousViewOptionIgnoreInvalidationOlderThan,
	ContinuousViewOptionMaterializedOnly,
} ContinuousAggViewOption;

typedef enum ContinuousAggViewType
{
	ContinuousAggUserView = 0,
	ContinuousAggPartialView,
	ContinuousAggDirectView,
	ContinuousAggNone
} ContinuousAggViewType;

extern TSDLLEXPORT WithClauseResult *ts_continuous_agg_with_clause_parse(const List *defelems);

typedef struct ContinuousAgg
{
	FormData_continuous_agg data;
} ContinuousAgg;

typedef enum ContinuousAggHypertableStatus
{
	HypertableIsNotContinuousAgg = 0,
	HypertableIsMaterialization = 1,
	HypertableIsRawTable = 2,
	HypertableIsMaterializationAndRaw = HypertableIsMaterialization | HypertableIsRawTable,
} ContinuousAggHypertableStatus;

typedef struct ContinuousAggMatOptions
{
	bool verbose;
	bool within_single_transaction;
	bool process_only_invalidation;
	int64 invalidate_prior_to_time; /* exclusive, if not bucketed, the last invalidation bucket will
									   cover this point */
} ContinuousAggMatOptions;

extern TSDLLEXPORT ContinuousAggHypertableStatus
ts_continuous_agg_hypertable_status(int32 hypertable_id);
extern TSDLLEXPORT List *ts_continuous_aggs_find_by_raw_table_id(int32 raw_hypertable_id);
extern TSDLLEXPORT int64 ts_continuous_aggs_max_ignore_invalidation_older_than(
	int32 raw_hypertable_id, FormData_continuous_agg *entry);
TSDLLEXPORT int64 ts_continuous_aggs_min_completed_threshold(int32 raw_hypertable_id,
															 FormData_continuous_agg *entry);
extern TSDLLEXPORT int64 ts_continuous_aggs_get_minimum_invalidation_time(
	int64 modification_time, int64 ignore_invalidation_older_than);
TSDLLEXPORT
int64 ts_continuous_agg_get_completed_threshold(int32 materialization_id);

extern TSDLLEXPORT ContinuousAgg *ts_continuous_agg_find_by_view_name(const char *schema,
																	  const char *name);

extern TSDLLEXPORT ContinuousAgg *ts_continuous_agg_find_by_job_id(int32 job_id);
extern void ts_continuous_agg_drop_view_callback(ContinuousAgg *ca, const char *schema,
												 const char *name);

extern void ts_continuous_agg_drop_hypertable_callback(int32 hypertable_id);

extern TSDLLEXPORT ContinuousAggViewType ts_continuous_agg_view_type(FormData_continuous_agg *data,
																	 const char *schema,
																	 const char *name);
extern void ts_continuous_agg_rename_schema_name(char *old_schema, char *new_schema);
extern void ts_continuous_agg_rename_view(char *old_schema, char *name, char *new_schema,
										  char *new_name);

extern TSDLLEXPORT int32 ts_number_of_continuous_aggs(void);

extern Oid ts_continuous_agg_get_user_view_oid(ContinuousAgg *agg);
extern TSDLLEXPORT Dimension *
ts_continuous_agg_find_integer_now_func_by_materialization_id(int32 mat_htid);
extern ContinuousAgg *ts_continuous_agg_find_userview_name(const char *schema, const char *name);

#endif /* TIMESCALEDB_CONTINUOUS_AGG_H */
