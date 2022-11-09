/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_DEBUG_GUC_H
#define TIMESCALEDB_DEBUG_GUC_H

#include <postgres.h>
#include <fmgr.h>
#include <utils/guc.h>

#include "export.h"

/*
 * Enable printout inside ts_create_upper based on the stage provided. It is
 * possible to enable printout for multiple stages, so we take the existing
 * stage list and create a mask from it.
 */
#define STAGE_SETOP (1UL << UPPERREL_SETOP) /* Enabled using "setop" */
#define STAGE_PARTIAL_GROUP_AGG                                                                    \
	(1UL << UPPERREL_PARTIAL_GROUP_AGG)				/* Enabled using "partial_group_agg" */
#define STAGE_GROUP_AGG (1UL << UPPERREL_GROUP_AGG) /* Enabled using "group_agg" */
#define STAGE_WINDOW (1UL << UPPERREL_WINDOW)		/* Enabled using "window" */
#define STAGE_DISTINCT (1UL << UPPERREL_DISTINCT)	/* Enabled using "distinct" */
#define STAGE_ORDERED (1UL << UPPERREL_ORDERED)		/* Enabled using "ordered" */
#define STAGE_FINAL (1UL << UPPERREL_FINAL)			/* Enabled using "final" */

/*
 * Debug flags for the optimizer.
 *
 * Add new flags here as you see fit, but don't forget to update the flag list
 * `flag_names` in guc.c.
 */
typedef struct DebugOptimizerFlags
{
	/* Bit mask to represent set of UpperRelationKind, which is used as the
	 * stage inside create_upper. */
	unsigned long show_upper;
	bool show_rel;
} DebugOptimizerFlags;

extern TSDLLEXPORT DebugOptimizerFlags ts_debug_optimizer_flags;

extern void ts_debug_init(void);

#endif /* TIMESCALEDB_DEBUG_GUC_H */
