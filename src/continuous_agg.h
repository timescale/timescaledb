/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_CONTINUOUS_AGGS_CONTIGUOUS_AGG_H
#define TIMESCALEDB_TSL_CONTINUOUS_AGGS_CONTIGUOUS_AGG_H
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

extern ContinuousAggHypertableStatus ts_continuous_agg_hypertable_status(int32 hypertable_id);
extern void ts_continuous_agg_drop_chunks_by_chunk_id(int32 raw_hypertable_id, Chunk **chunks,
													  Size num_chunks);
extern TSDLLEXPORT List *ts_continuous_aggs_find_by_raw_table_id(int32 raw_hypertable_id);
extern TSDLLEXPORT ContinuousAgg *ts_continuous_agg_find_by_view_name(const char *schema,
																	  const char *name);
extern void ts_continuous_agg_drop_view_callback(ContinuousAgg *ca, const char *schema,
												 const char *name);

extern void ts_continuous_agg_drop_hypertable_callback(int32 hypertable_id);

extern TSDLLEXPORT ContinuousAggViewType ts_continuous_agg_view_type(FormData_continuous_agg *data,
																	 const char *schema,
																	 const char *name);
extern void ts_continuous_agg_rename_schema_name(char *old_schema, char *new_schema);
extern void ts_continuous_agg_rename_view(char *old_schema, char *name, char *new_schema,
										  char *new_name);

#endif /* TIMESCALEDB_TSL_CONTINUOUS_AGGS_CONTIGUOUS_AGG_H */
