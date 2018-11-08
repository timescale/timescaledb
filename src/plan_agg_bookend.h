/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TIMESCALEDB_PLAN_AGG_BOOKEND_H
#define TIMESCALEDB_PLAN_AGG_BOOKEND_H

#include <nodes/relation.h>
#include <nodes/pg_list.h>

extern void
			ts_preprocess_first_last_aggregates(PlannerInfo *root, List *tlist);
#endif							/* TIMESCALEDB_PLAN_AGG_BOOKEND_H */
