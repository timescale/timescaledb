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

typedef struct ParallelChunkAppendState
{
	int next_plan;
	bool finished[FLEXIBLE_ARRAY_MEMBER];
} ParallelChunkAppendState;

typedef struct ChunkAppendState
{
	CustomScanState csstate;
	PlanState **subplanstates;

	MemoryContext exclusion_ctx;

	int num_subplans;
	int first_partial_plan;
	int filtered_first_partial_plan;
	int current;

	Oid ht_reloid;
	bool startup_exclusion;
	bool runtime_exclusion;
	bool runtime_initialized;
	uint32 limit;

	/* list of subplans after planning */
	List *initial_subplans;
	/* list of constraints indexed like initial_subplans */
	List *initial_constraints;
	/* list of restrictinfo clauses indexed like initial_subplans */
	List *initial_ri_clauses;

	/* list of subplans after startup exclusion */
	List *filtered_subplans;
	/* list of relation constraints after startup exclusion */
	List *filtered_constraints;
	/* list of restrictinfo clauses after startup exclusion */
	List *filtered_ri_clauses;

	/* valid subplans for runtime exclusion */
	Bitmapset *valid_subplans;
	Bitmapset *params;

	/* sort options if this append is ordered, only used for EXPLAIN */
	List *sort_options;

	/* number of loops and exclusions for EXPLAIN */
	int runtime_number_loops;
	int runtime_number_exclusions;

	LWLock *lock;
	ParallelContext *pcxt;
	ParallelChunkAppendState *pstate;
	void (*choose_next_subplan)(struct ChunkAppendState *);

#if PG12_GE
	TupleTableSlot *slot;
#endif
} ChunkAppendState;

extern Node *ts_chunk_append_state_create(CustomScan *cscan);

#endif /* TIMESCALEDB_CHUNK_APPEND_EXEC_H */
