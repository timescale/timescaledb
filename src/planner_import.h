#ifndef TIMESCALEDB_PLANNER_IMPORT_H
#define TIMESCALEDB_PLANNER_IMPORT_H

#include <postgres.h>
#include <utils/lsyscache.h>
#include <utils/relcache.h>

/*
 * This file contains functions copied verbatim from the PG core planner.
 * These function had to be copied since they were declared static in the core planner, but we need them for our
 * manipulations.
 */

extern void make_inh_translation_list(Relation oldrelation, Relation newrelation,
						  Index newvarno,
						  List **translated_vars);

#endif							/* TIMESCALEDB_PLANNER_IMPORT_H */
