/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TIMESCALEDB_CHUNK_DISPATCH_PLAN_H
#define TIMESCALEDB_CHUNK_DISPATCH_PLAN_H

#include <postgres.h>
#include <nodes/plannodes.h>
#include <nodes/parsenodes.h>
#include <nodes/extensible.h>

extern CustomScan *ts_chunk_dispatch_plan_create(Plan *subplan, Index hypertable_rti, Oid hypertable_relid, Query *parse);

#endif							/* TIMESCALEDB_CHUNK_DISPATCH_PLAN_H */
