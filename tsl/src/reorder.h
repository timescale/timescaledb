/*
 * Copyright (c) 2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Timescale License,
 * see LICENSE-TIMESCALE at the top of the tsl directory.
 */

#ifndef TIMESCALEDB_TSL_REORDER_H
#define TIMESCALEDB_TSL_REORDER_H

#include <postgres.h>
#include <fmgr.h>
#include <nodes/parsenodes.h>
#include <storage/lock.h>
#include <utils/relcache.h>

extern Datum tsl_reorder_chunk(PG_FUNCTION_ARGS);
extern void reorder_chunk(Oid chunk_id, Oid index_id, bool verbose, Oid wait_id);

#endif							/* TIMESCALEDB_TSL_REORDER_H */
