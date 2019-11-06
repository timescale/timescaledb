/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_PLAN_AGG_BOOKEND_H
#define TIMESCALEDB_PLAN_AGG_BOOKEND_H

#include "compat.h"

#include <nodes/primnodes.h>
#if PG12_LT /* nodes/relation.h renamed in fa2cf16 */
#include <nodes/relation.h>
#else
#include <nodes/pathnodes.h>
#endif

extern void ts_preprocess_first_last_aggregates(PlannerInfo *root, List *tlist);
#endif /* TIMESCALEDB_PLAN_AGG_BOOKEND_H */
