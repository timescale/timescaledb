/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_CHUNK_APPEND_EXEC_H
#define TIMESCALEDB_CHUNK_APPEND_EXEC_H

#include <postgres.h>
#include <nodes/bitmapset.h>
#include <nodes/extensible.h>
#include <nodes/relation.h>

typedef struct ChunkAppendState
{
	CustomScanState csstate;
	PlanState **subplanstates;
	int num_subplans;
	int current;

	Oid ht_reloid;
	bool startup_exclusion;
	bool runtime_exclusion;
	bool runtime_initialized;
	uint32 limit;

	/* list of subplans after planning */
	List *initial_subplans;
	/* list of restrictinfo clauses indexed similar to initial_subplans */
	List *initial_ri_clauses;

	/* list of subplans after startup exclusion */
	List *filtered_subplans;
	/* list of restrictinfo clauses after startup exclusion */
	List *filtered_ri_clauses;

	/* valid subplans for runtime exclusion */
	Bitmapset *valid_subplans;

} ChunkAppendState;

extern Node *chunk_append_state_create(CustomScan *cscan);

#endif /* TIMESCALEDB_CHUNK_APPEND_EXEC_H */
