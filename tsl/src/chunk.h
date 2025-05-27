/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <chunk.h>
#include <fmgr.h>

extern Datum chunk_freeze_chunk(PG_FUNCTION_ARGS);
extern Datum chunk_unfreeze_chunk(PG_FUNCTION_ARGS);
extern int chunk_invoke_drop_chunks(Oid relid, Datum older_than, Datum older_than_type,
									bool use_creation_time);
extern Datum chunk_merge_chunks(PG_FUNCTION_ARGS);
extern Datum chunk_split_chunk(PG_FUNCTION_ARGS);
extern void update_relstats(Relation catrel, Oid relid, BlockNumber num_pages, double ntuples);
extern void compute_rel_vacuum_cutoffs(Relation rel, struct VacuumCutoffs *cutoffs);
extern void chunk_update_constraints(const Chunk *chunk, const Hypercube *new_cube);
