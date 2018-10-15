/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TIMESCALEDB_PLANNER_UTILS_H
#define TIMESCALEDB_PLANNER_UTILS_H

#include <postgres.h>
#include <nodes/plannodes.h>

extern void planned_stmt_walker(PlannedStmt *stmt, void (*walker) (Plan **, void *), void *context);

#endif							/* TIMESCALEDB_PLANNER_UTILS_H */
