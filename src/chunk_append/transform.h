/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_CHUNK_APPEND_TRANSFORM_H
#define TIMESCALEDB_CHUNK_APPEND_TRANSFORM_H

#include <postgres.h>
#include <nodes/extensible.h>

#include "compat.h"

#if PG12_LT /* nodes/relation.h renamed in fa2cf16 */
#include <nodes/relation.h>
#else
#include <nodes/pathnodes.h>
#endif

extern Expr *ts_transform_cross_datatype_comparison(Expr *clause);

#endif /* TIMESCALEDB_CHUNK_APPEND_TRANSFORM_H */
