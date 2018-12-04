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

typedef struct HypertableInsertState
{
	CustomScanState cscan_state;
	ModifyTable *mt;
} HypertableInsertState;

extern Plan *ts_hypertable_insert_plan_create(ModifyTable *mt);

#endif							/* TIMESCALEDB_HYPERTABLE_INSERT_H */
