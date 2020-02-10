/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_NODES_GAPFILL_EXEC_H
#define TIMESCALEDB_TSL_NODES_GAPFILL_EXEC_H

#include <postgres.h>
#include <nodes/execnodes.h>

/*
 * GapFillFetchState describes the state of subslot in GapFillState:
 * FETCHED_NONE: no tuple in subslot
 * FETCHED_ONE: valid tuple in subslot
 * FETCHED_LAST: no tuple in subslot and no more tuples from subplan
 * FETCHED_NEXT_GROUP: tuple in subslot belongs to next aggregation group
 *
 * The start state is FETCHED_NONE and the end state is FETCHED_LAST
 *
 *  State transition with single group by time
 *
 *                                     no tuple returned
 *  FETCHED_NONE --> fetch_next_tuple -------------------> FETCHED_LAST
 *            ^               |
 *            |               | tuple found
 *            |               |
 *            |               v
 *            └----------FETCHED_ONE
 *       tuple returned
 *
 *  State transition with multiple groups
 *
 *                                     no tuple returned
 *  FETCHED_NONE --> fetch_next_tuple -------------------> FETCHED_LAST
 *            ^               |
 *            |               | tuple found
 *            |               v
 *            |       check_group_changed -------> FETCHED_NEXT_GROUP
 *            |               |             yes        |
 *            |               | no                     |
 *            |               |                        |
 *            |               v                        |
 *            └----------FETCHED_ONE <-----------------
 *       tuple returned
 */
typedef enum GapFillFetchState
{
	FETCHED_NONE,
	FETCHED_ONE,
	FETCHED_NEXT_GROUP,
	FETCHED_LAST,
} GapFillFetchState;

/*
 * NULL_COLUMN: column with no special action from gapfill e.g. min(value)
 * TIME_COLUMN: column with time_bucket_gapfill call
 * GROUP_COLUMN: any column appearing in GROUP BY clause
 * DERIVED_COLUMN: column not appearing in GROUP BY but dependent on GROUP BY column
 * LOCF_COLUMN: column with locf call
 * INTERPOLATE_COLUMN: column with interpolate call
 */
typedef enum GapFillColumnType
{
	NULL_COLUMN,
	TIME_COLUMN,
	GROUP_COLUMN,
	DERIVED_COLUMN,
	LOCF_COLUMN,
	INTERPOLATE_COLUMN
} GapFillColumnType;

typedef struct GapFillColumnState
{
	GapFillColumnType ctype;
	Oid typid;
	bool typbyval;
	int16 typlen;
} GapFillColumnState;

typedef struct GapFillGroupColumnState
{
	GapFillColumnState base;
	Datum value;
	bool isnull;
} GapFillGroupColumnState;

typedef struct GapFillState
{
	CustomScanState csstate;
	Plan *subplan;

	Oid gapfill_typid;
	int64 gapfill_start;
	int64 gapfill_end;
	int64 gapfill_period;

	int64 next_timestamp;
	int64 subslot_time; /* time of tuple in subslot */

	int time_index;			 /* position of time column */
	TupleTableSlot *subslot; /* TupleTableSlot storing data from subplan */

	bool multigroup; /* multiple groupings */
	bool groups_initialized;

	int ncolumns;
	GapFillColumnState **columns;

	ProjectionInfo *pi;
	TupleTableSlot *scanslot;
	GapFillFetchState state;
} GapFillState;

Node *gapfill_state_create(CustomScan *);
Expr *gapfill_adjust_varnos(GapFillState *state, Expr *expr);
Datum gapfill_exec_expr(GapFillState *state, Expr *expr, bool *isnull);
int64 gapfill_datum_get_internal(Datum, Oid);

#endif /* TIMESCALEDB_TSL_NODES_GAPFILL_EXEC_H */
