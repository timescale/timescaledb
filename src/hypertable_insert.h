/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TIMESCALEDB_HYPERTABLE_INSERT_H
#define TIMESCALEDB_HYPERTABLE_INSERT_H

#include <postgres.h>
#include <nodes/execnodes.h>
#include <foreign/fdwapi.h>

#include "hypertable.h"

typedef struct HypertableInsertPath
{
	CustomPath	cpath;
} HypertableInsertPath;

typedef struct HypertableInsertState
{
	CustomScanState cscan_state;
	ModifyTable *mt;
} HypertableInsertState;

extern Plan *ts_hypertable_insert_fixup_tlist(Plan *plan);
extern Path *ts_hypertable_insert_path_create(PlannerInfo *root, ModifyTablePath *mtpath);

#endif							/* TIMESCALEDB_HYPERTABLE_INSERT_H */
