/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_ESTIMATE_H
#define TIMESCALEDB_ESTIMATE_H

#include <postgres.h>

#define INVALID_ESTIMATE (-1)
#define IS_VALID_ESTIMATE(est) ((est) >= 0)

extern double ts_estimate_group_expr_interval(PlannerInfo *root, Expr *expr,
											  double interval_period);
extern double ts_estimate_group(PlannerInfo *root, double path_rows);

#endif /* TIMESCALEDB_ESTIMATE_H */
