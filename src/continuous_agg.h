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
#include "with_clause_parser.h"
#include "compat.h"
#define CAGGINVAL_TRIGGER_NAME "ts_cagg_invalidation_trigger"

typedef enum ContinuousAggViewOption
{
	ContinuousEnabled = 0,
	ContinuousViewOptionRefreshLag,
	ContinuousViewOptionRefreshInterval,
} ContinuousAggViewOption;

extern TSDLLEXPORT WithClauseResult *ts_continuous_agg_with_clause_parse(const List *defelems);

typedef struct ContinuousAgg
{
	FormData_continuous_agg data;
} ContinuousAgg;

extern TSDLLEXPORT ContinuousAgg *ts_continuous_agg_find_by_view_name(const char *schema,
																	  const char *name);
extern void ts_continuous_agg_drop_view_callback(ContinuousAgg *ca, const char *schema,
												 const char *name);

extern void ts_continuous_agg_drop_hypertable_callback(int32 hypertable_id);

extern TSDLLEXPORT bool ts_continuous_agg_is_user_view(FormData_continuous_agg *data,
													   const char *schema, const char *name);
extern TSDLLEXPORT bool ts_continuous_agg_is_partial_view(FormData_continuous_agg *data,
														  const char *schema, const char *name);

extern void ts_continuous_agg_rename_schema_name(char *old_schema, char *new_schema);
extern void ts_continuous_agg_rename_view(char *old_schema, char *name, char *new_schema,
										  char *new_name);

#endif /* TIMESCALEDB_TSL_CONTINUOUS_AGGS_CONTIGUOUS_AGG_H */
