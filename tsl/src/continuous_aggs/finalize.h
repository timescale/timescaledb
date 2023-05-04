/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_CONTINUOUS_AGGS_FINALIZE_H
#define TIMESCALEDB_TSL_CONTINUOUS_AGGS_FINALIZE_H

#include <postgres.h>

#include <catalog/pg_aggregate.h>
#include <catalog/pg_collation.h>
#include <catalog/pg_type.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/pg_list.h>
#include <parser/parse_func.h>
#include <utils/builtins.h>
#include <utils/regproc.h>
#include <utils/syscache.h>

#include "ts_catalog/catalog.h"
#include "common.h"

#define FINALFN "finalize_agg"

extern Oid get_finalize_function_oid(void);
extern Aggref *get_finalize_aggref(Aggref *inp, Var *partial_state_var);
extern Node *add_aggregate_partialize_mutator(Node *node, AggPartCxt *cxt);
extern Node *add_var_mutator(Node *node, AggPartCxt *cxt);
extern Node *finalize_query_create_having_qual(FinalizeQueryInfo *inp,
											   MatTableColumnInfo *mattblinfo);
extern Query *finalize_query_get_select_query(FinalizeQueryInfo *inp, List *matcollist,
											  ObjectAddress *mattbladdress);
extern void finalizequery_init(FinalizeQueryInfo *inp, Query *orig_query,
							   MatTableColumnInfo *mattblinfo);
extern Query *finalizequery_get_select_query(FinalizeQueryInfo *inp, List *matcollist,
											 ObjectAddress *mattbladdress, char *relname);
#endif /* TIMESCALEDB_TSL_CONTINUOUS_AGGS_FINALIZE_H */
