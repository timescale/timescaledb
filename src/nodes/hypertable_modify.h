/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_HYPERTABLE_MODIFY_H
#define TIMESCALEDB_HYPERTABLE_MODIFY_H

#include <postgres.h>
#include <nodes/execnodes.h>
#include <foreign/fdwapi.h>

#include "hypertable.h"

typedef struct HypertableModifyPath
{
	CustomPath cpath;
	/* A bitmapset to remember which subpaths are using data node dispatching. */
	Bitmapset *distributed_insert_plans;
	/* List of server oids for the hypertable's data nodes */
	List *serveroids;
} HypertableModifyPath;

typedef struct HypertableModifyState
{
	CustomScanState cscan_state;
	ModifyTable *mt;
	List *serveroids;
	FdwRoutine *fdwroutine;
} HypertableModifyState;

extern void ts_hypertable_modify_fixup_tlist(Plan *plan);
extern Path *ts_hypertable_modify_path_create(PlannerInfo *root, ModifyTablePath *mtpath,
											  Hypertable *ht);

#endif /* TIMESCALEDB_HYPERTABLE_MODIFY_H */
