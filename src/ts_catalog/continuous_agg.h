/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#ifndef TIMESCALEDB_CONTINUOUS_AGG_H
#define TIMESCALEDB_CONTINUOUS_AGG_H
#include <postgres.h>
#include <catalog/pg_type.h>

#include "ts_catalog/catalog.h"
#include "chunk.h"

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

#define BUCKET_WIDTH_VARIABLE (-1)

/*
 * Information about the bucketing function.
 *
 * Note that this structure should be serializable and it should be possible
 * to transfer it over the network to the data nodes. The following procedures
 * are responsible for serializing and deserializing respectively:
 *
 * - bucket_function_serialize()
 * - bucket_function_deserialize()
 *
 * Serialized data is used as an input of the following procedures:
 *
 * - _timescaledb_internal.invalidation_process_hypertable_log()
 * - _timescaledb_internal.invalidation_process_cagg_log()
 *
 * See bucket_functions[] argument.
 */
typedef struct ContinuousAggsBucketFunction
{
	/*
	 * Schema of the bucketing function.
	 * Equals TRUE for "timescaledb_experimental", FALSE otherwise.
	 */
	bool experimental;
	/* Name of the bucketing function, e.g. "time_bucket" or "time_bucket_ng" */
	char *name;
	/* `bucket_width` argument of the function */
	Interval *bucket_width;
	/*
	 * Custom origin value stored as UTC timestamp.
	 * If not specified, stores infinity.
	 */
	Timestamp origin;
	/* `timezone` argument of the function provided by the user */
	char *timezone;
} ContinuousAggsBucketFunction;

typedef struct ContinuousAgg
{
	FormData_continuous_agg data;

	/*
	 * bucket_function is NULL unless the bucket is variable in size,
	 * e.g. monthly bucket or a bucket with a timezone.
	 * In this case data.bucket_with stores BUCKET_WIDTH_VARIABLE.
	 */
	ContinuousAggsBucketFunction *bucket_function;

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

typedef struct CaggsInfoData
{
	/* (int32) elements */
	List *mat_hypertable_ids;
	/* (int64) Datum elements; stores BUCKET_WIDTH_VARIABLE for variable buckets */
	List *bucket_widths;
	/* (const ContinuousAggsBucketFunction *) elements; stores NULL for fixed buckets */
	List *bucket_functions;
} CaggsInfo;

extern TSDLLEXPORT const CaggsInfo ts_continuous_agg_get_all_caggs_info(int32 raw_hypertable_id);
extern TSDLLEXPORT void ts_populate_caggs_info_from_arrays(ArrayType *mat_hypertable_ids,
														   ArrayType *bucket_widths,
														   ArrayType *bucket_functions,
														   CaggsInfo *all_caggs);
TSDLLEXPORT void ts_create_arrays_from_caggs_info(const CaggsInfo *all_caggs,
												  ArrayType **mat_hypertable_ids,
												  ArrayType **bucket_widths,
												  ArrayType **bucket_functions);

extern TSDLLEXPORT ContinuousAgg *
ts_continuous_agg_find_by_mat_hypertable_id(int32 mat_hypertable_id);

extern TSDLLEXPORT void ts_materialization_invalidation_log_delete_inner(int32 materialization_id);

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

extern TSDLLEXPORT const Dimension *
ts_continuous_agg_find_integer_now_func_by_materialization_id(int32 mat_htid);
extern ContinuousAgg *ts_continuous_agg_find_userview_name(const char *schema, const char *name);

extern TSDLLEXPORT bool ts_continuous_agg_bucket_width_variable(const ContinuousAgg *agg);
extern TSDLLEXPORT int64 ts_continuous_agg_bucket_width(const ContinuousAgg *agg);

extern TSDLLEXPORT void
ts_compute_inscribed_bucketed_refresh_window_variable(int64 *start, int64 *end,
													  const ContinuousAggsBucketFunction *bf);
extern TSDLLEXPORT void
ts_compute_circumscribed_bucketed_refresh_window_variable(int64 *start, int64 *end,
														  const ContinuousAggsBucketFunction *bf);
extern TSDLLEXPORT int64 ts_compute_beginning_of_the_next_bucket_variable(
	int64 timeval, const ContinuousAggsBucketFunction *bf);

#endif /* TIMESCALEDB_CONTINUOUS_AGG_H */
