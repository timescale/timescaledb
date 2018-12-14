/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */

/*
 * This file contains source code that was copied and/or modified from
 * the PostgreSQL database, which is licensed under the open-source
 * PostgreSQL License. Please see the NOTICE at the top level
 * directory for a copy of the PostgreSQL License.
 *
 * These function were copied from the PostgreSQL core planner, since
 * they were declared static in the core planner, but we need them for
 * our manipulations.
 */
#ifndef TIMESCALEDB_PLANNER_IMPORT_H
#define TIMESCALEDB_PLANNER_IMPORT_H

#include <postgres.h>
#include <utils/lsyscache.h>
#include <utils/relcache.h>
#include <utils/selfuncs.h>

extern void ts_make_inh_translation_list(Relation oldrelation, Relation newrelation,
							 Index newvarno,
							 List **translated_vars);
extern size_t ts_estimate_hashagg_tablesize(struct Path *path,
							  const struct AggClauseCosts *agg_costs,
							  double dNumGroups);

extern struct PathTarget *ts_make_partial_grouping_target(struct PlannerInfo *root,
								PathTarget *grouping_target);

extern bool ts_get_variable_range(PlannerInfo *root, VariableStatData *vardata, Oid sortop,
					  Datum *min, Datum *max);

#endif							/* TIMESCALEDB_PLANNER_IMPORT_H */
