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
#include "compat/compat.h"

#define CAGGINVAL_TRIGGER_NAME "ts_cagg_invalidation_trigger"

typedef enum ContinuousAggViewOption
{
	ContinuousEnabled = 0,
	ContinuousViewOptionCreateGroupIndex,
	ContinuousViewOptionMaterializedOnly,
	ContinuousViewOptionCompress,
} ContinuousAggViewOption;

typedef enum ContinuousAggViewType
{
	ContinuousAggUserView = 0,
	ContinuousAggPartialView,
	ContinuousAggDirectView,
	ContinuousAggAnyView
} ContinuousAggViewType;

extern TSDLLEXPORT WithClauseResult *ts_continuous_agg_with_clause_parse(const List *defelems);

typedef struct ContinuousAgg
{
	FormData_continuous_agg data;
	/* Relid of the user-facing view */
	Oid relid;
	/* Type of the primary partitioning dimension */
	Oid partition_type;
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

extern TSDLLEXPORT ContinuousAgg *
ts_continuous_agg_find_by_mat_hypertable_id(int32 mat_hypertable_id);

extern TSDLLEXPORT ContinuousAggHypertableStatus
ts_continuous_agg_hypertable_status(int32 hypertable_id);
extern TSDLLEXPORT List *ts_continuous_aggs_find_by_raw_table_id(int32 raw_hypertable_id);
extern TSDLLEXPORT ContinuousAgg *ts_continuous_agg_find_by_view_name(const char *schema,
																	  const char *name,
																	  ContinuousAggViewType type);
extern TSDLLEXPORT ContinuousAgg *ts_continuous_agg_find_by_relid(Oid relid);
extern TSDLLEXPORT ContinuousAgg *ts_continuous_agg_find_by_rv(const RangeVar *rv);

extern bool ts_continuous_agg_drop(const char *view_schema, const char *view_name);
extern void ts_continuous_agg_drop_hypertable_callback(int32 hypertable_id);

extern TSDLLEXPORT ContinuousAggViewType ts_continuous_agg_view_type(FormData_continuous_agg *data,
																	 const char *schema,
																	 const char *name);
extern void ts_continuous_agg_rename_schema_name(char *old_schema, char *new_schema);
extern void ts_continuous_agg_rename_view(const char *old_schema, const char *name,
										  const char *new_schema, const char *new_name,
										  ObjectType *object_type);

extern TSDLLEXPORT int32 ts_number_of_continuous_aggs(void);

extern Oid ts_continuous_agg_get_user_view_oid(ContinuousAgg *agg);
extern TSDLLEXPORT const Dimension *
ts_continuous_agg_find_integer_now_func_by_materialization_id(int32 mat_htid);
extern ContinuousAgg *ts_continuous_agg_find_userview_name(const char *schema, const char *name);

extern TSDLLEXPORT int64 ts_continuous_agg_bucket_width(const ContinuousAgg *agg);
extern TSDLLEXPORT int64 ts_continuous_agg_max_bucket_width(const ContinuousAgg *agg);

#endif /* TIMESCALEDB_CONTINUOUS_AGG_H */
