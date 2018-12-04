/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TIMESCALEDB_PLANNER_IMPORT_H
#define TIMESCALEDB_PLANNER_IMPORT_H

#include <postgres.h>
#include <utils/lsyscache.h>
#include <utils/relcache.h>
#include <utils/selfuncs.h>

/*
 * This file contains functions copied verbatim from the PG core planner.
 * These function had to be copied since they were declared static in the core planner, but we need them for our
 * manipulations.
 */

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
