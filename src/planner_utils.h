#ifndef TIMESCALEDB_PLANNER_UTILS_H
#define TIMESCALEDB_PLANNER_UTILS_H

#include <postgres.h>
#include <nodes/plannodes.h>

extern void planned_stmt_walker(PlannedStmt *stmt, void (*walker) (Plan **, void *), void *context);

#endif							/* TIMESCALEDB_PLANNER_UTILS_H */
