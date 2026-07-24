/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <nodes/parsenodes.h>
#include <nodes/pathnodes.h>

#include "hypertable.h"

extern bool ts_should_deferred_chunk_scan(const Query *query, const Hypertable *ht);
extern void ts_deferred_chunk_scan_add_path(PlannerInfo *root, RelOptInfo *rel, const Hypertable *ht);
extern void _deferred_chunk_scan_init(void);
