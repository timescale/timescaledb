/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_SORT_TRANSFORM_H
#define TIMESCALEDB_SORT_TRANSFORM_H

#include <postgres.h>

extern Expr *ts_sort_transform_expr(Expr *expr);

#endif /* TIMESCALEDB_SORT_TRANSFORM_H */
