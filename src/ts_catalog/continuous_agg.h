/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <catalog/pg_type.h>
#include <nodes/parsenodes.h>

#include "chunk.h"
#include "hypertable.h"
#include "ts_catalog/catalog.h"

#include "compat/compat.h"
#include "with_clause_parser.h"

#define CAGGINVAL_TRIGGER_NAME "ts_cagg_invalidation_trigger"

/*switch to ts user for _timescaledb_internal access */
#define SWITCH_TO_TS_USER(schemaname, newuid, saved_uid, saved_secctx)                             \
	do                                                                                             \
	{                                                                                              \
		if ((schemaname) &&                                                                        \
			strncmp(schemaname, INTERNAL_SCHEMA_NAME, strlen(INTERNAL_SCHEMA_NAME)) == 0)          \
			(newuid) = ts_catalog_database_info_get()->owner_uid;                                  \
		else                                                                                       \
			(newuid) = InvalidOid;                                                                 \
		if (OidIsValid((newuid)))                                                                  \
		{                                                                                          \
			GetUserIdAndSecContext(&(saved_uid), &(saved_secctx));                                 \
			SetUserIdAndSecContext(uid, (saved_secctx) | SECURITY_LOCAL_USERID_CHANGE);            \
		}                                                                                          \
	} while (0)

#define RESTORE_USER(newuid, saved_uid, saved_secctx)                                              \
	do                                                                                             \
	{                                                                                              \
		if (OidIsValid((newuid)))                                                                  \
			SetUserIdAndSecContext(saved_uid, saved_secctx);                                       \
	} while (0);

/* Does the function belong to a time_bucket_ng function that is no longer allowed
 * in CAgg definitions? */
#define IS_DEPRECATED_TIME_BUCKET_NG_FUNC(funcinfo)                                                \
	((funcinfo->origin == ORIGIN_TIMESCALE_EXPERIMENTAL) &&                                        \
	 (strcmp("time_bucket_ng", funcinfo->funcname) == 0))

typedef enum ContinuousAggViewOption
{
	ContinuousEnabled = 0,
	ContinuousViewOptionCreateGroupIndex,
	ContinuousViewOptionMaterializedOnly,
	ContinuousViewOptionCompress,
	ContinuousViewOptionFinalized,
	ContinuousViewOptionCompressSegmentBy,
	ContinuousViewOptionCompressOrderBy,
	ContinuousViewOptionCompressChunkTimeInterval,
	ContinuousViewOptionMax
} ContinuousAggViewOption;

typedef enum ContinuousAggViewType
{
	ContinuousAggUserView = 0,
	ContinuousAggPartialView,
	ContinuousAggDirectView,
	ContinuousAggAnyView
} ContinuousAggViewType;

extern TSDLLEXPORT WithClauseResult *ts_continuous_agg_with_clause_parse(const List *defelems);

extern TSDLLEXPORT List *
ts_continuous_agg_get_compression_defelems(const WithClauseResult *with_clauses);

/*
 * Information about the bucketing function.
 */
typedef struct ContinuousAggsBucketFunction
{
	/* Oid of the bucketing function. In the catalog table, the regprocedure is used. This ensures
	 * that the Oid is mapped to a string when a backup is taken and the string is converted back to
	 * the Oid when the backup is restored. This way, we can use an Oid in the catalog table even
	 * when a backup is restored and the Oid may have changed. However, the dependency management in
	 * PostgreSQL does not track the Oid. If the function is dropped and a new one is created, the
	 * Oid changes and this value points to a non-existing Oid. This can not happen in real-world
	 * situations since PostgreSQL protects the bucket_function from deletion until the CAgg is
	 * defined. */
	Oid bucket_function;
	Oid bucket_width_type; /* type of bucket_width */

	/* Is the interval of the bucket fixed? */
	bool bucket_fixed_interval;

	/* Is the bucket defined on a time datatype ?*/
	bool bucket_time_based;

	/*
	 * Fields that are used for time based buckets
	 */
	Interval *bucket_time_width;
	/*
	 * Custom origin value stored as UTC timestamp.
	 * If not specified, stores infinity.
	 */
	TimestampTz bucket_time_origin;
	Interval *bucket_time_offset;
	char *bucket_time_timezone;

	/*
	 * Fields that are used on integer based buckets
	 */
	int64 bucket_integer_width;
	int64 bucket_integer_offset;

} ContinuousAggsBucketFunction;

typedef struct ContinuousAgg
{
	FormData_continuous_agg data;

	/* Info about the time bucketing function */
	ContinuousAggsBucketFunction *bucket_function;

	/* Relid of the user-facing view */
	Oid relid;

	/* Type of the primary partitioning dimension */
	Oid partition_type;
} ContinuousAgg;

static inline bool
ContinuousAggIsFinalized(const ContinuousAgg *cagg)
{
	return (cagg->data.finalized == true);
}

static inline bool
ContinuousAggIsHierarchical(const ContinuousAgg *cagg)
{
	return (cagg->data.parent_mat_hypertable_id != INVALID_HYPERTABLE_ID);
}

typedef enum ContinuousAggHypertableStatus
{
	HypertableIsNotContinuousAgg = 0,
	HypertableIsMaterialization = 1,
	HypertableIsRawTable = 2,
	HypertableIsMaterializationAndRaw = HypertableIsMaterialization | HypertableIsRawTable,
} ContinuousAggHypertableStatus;

typedef struct CaggsInfoData
{
	/* (int32) elements */
	List *mat_hypertable_ids;
	/* (const ContinuousAggsBucketFunction *) elements; stores NULL for fixed buckets */
	List *bucket_functions;
} CaggsInfo;

typedef struct CaggPolicyOffset
{
	Datum value;
	Oid type;
	bool isnull;
	const char *name;
} CaggPolicyOffset;

extern TSDLLEXPORT Oid ts_cagg_permissions_check(Oid cagg_oid, Oid userid);

extern TSDLLEXPORT CaggsInfo ts_continuous_agg_get_all_caggs_info(int32 raw_hypertable_id);
extern TSDLLEXPORT ContinuousAgg *
ts_continuous_agg_find_by_mat_hypertable_id(int32 mat_hypertable_id, bool missing_ok);

extern TSDLLEXPORT void ts_materialization_invalidation_log_delete_inner(int32 mat_hypertable_id);

extern TSDLLEXPORT ContinuousAggHypertableStatus
ts_continuous_agg_hypertable_status(int32 hypertable_id);
extern TSDLLEXPORT bool ts_continuous_agg_hypertable_all_finalized(int32 raw_hypertable_id);
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
extern TSDLLEXPORT void ts_continuous_agg_rename_schema_name(const char *old_schema,
															 const char *new_schema);
extern TSDLLEXPORT void ts_continuous_agg_rename_view(const char *old_schema, const char *old_name,
													  const char *new_schema, const char *new_name,
													  ObjectType *object_type);

extern TSDLLEXPORT const Dimension *
ts_continuous_agg_find_integer_now_func_by_materialization_id(int32 mat_htid);
extern ContinuousAgg *ts_continuous_agg_find_userview_name(const char *schema, const char *name);

extern TSDLLEXPORT void ts_continuous_agg_invalidate_chunk(Hypertable *ht, Chunk *chunk);

extern TSDLLEXPORT bool ts_continuous_agg_bucket_on_interval(Oid bucket_function);

extern TSDLLEXPORT void
ts_compute_inscribed_bucketed_refresh_window_variable(int64 *start, int64 *end,
													  const ContinuousAggsBucketFunction *bf);
extern TSDLLEXPORT void
ts_compute_circumscribed_bucketed_refresh_window_variable(int64 *start, int64 *end,
														  const ContinuousAggsBucketFunction *bf);
extern TSDLLEXPORT int64 ts_compute_beginning_of_the_next_bucket_variable(
	int64 timeval, const ContinuousAggsBucketFunction *bf);

extern TSDLLEXPORT Query *ts_continuous_agg_get_query(ContinuousAgg *cagg);

extern TSDLLEXPORT int64
ts_continuous_agg_fixed_bucket_width(const ContinuousAggsBucketFunction *bucket_function);
