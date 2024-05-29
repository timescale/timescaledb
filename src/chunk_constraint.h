/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <nodes/pg_list.h>

#include "hypertable.h"
#include "ts_catalog/catalog.h"

typedef struct ChunkConstraint
{
	FormData_chunk_constraint fd;
} ChunkConstraint;

typedef struct ChunkConstraints
{
	MemoryContext mctx;
	int16 capacity;
	int16 num_constraints;
	int16 num_dimension_constraints;
	ChunkConstraint *constraints;
} ChunkConstraints;

#define chunk_constraints_get(cc, i) &((cc)->constraints[i])

#define is_dimension_constraint(cc) ((cc)->fd.dimension_slice_id > 0)

typedef struct Chunk Chunk;
typedef struct DimensionSlice DimensionSlice;
typedef struct Hypercube Hypercube;
typedef struct ChunkScanCtx ChunkScanCtx;

extern TSDLLEXPORT ChunkConstraints *ts_chunk_constraints_alloc(int size_hint, MemoryContext mctx);
extern ChunkConstraints *
ts_chunk_constraint_scan_by_chunk_id(int32 chunk_id, Size num_constraints_hint, MemoryContext mctx);
extern ChunkConstraints *ts_chunk_constraints_copy(ChunkConstraints *chunk_constraints);
extern int ts_chunk_constraint_scan_by_dimension_slice(const DimensionSlice *slice,
													   ChunkScanCtx *ctx, MemoryContext mctx);
extern int ts_chunk_constraint_scan_by_dimension_slice_to_list(const DimensionSlice *slice,
															   List **list, MemoryContext mctx);
extern int ts_chunk_constraint_scan_by_dimension_slice_id(int32 dimension_slice_id,
														  ChunkConstraints *ccs,
														  MemoryContext mctx);
extern ChunkConstraint *ts_chunk_constraints_add(ChunkConstraints *ccs, int32 chunk_id,
												 int32 dimension_slice_id,
												 const char *constraint_name,
												 const char *hypertable_constraint_name);
extern int ts_chunk_constraints_add_dimension_constraints(ChunkConstraints *ccs, int32 chunk_id,
														  const Hypercube *cube);
extern TSDLLEXPORT int ts_chunk_constraints_add_inheritable_constraints(ChunkConstraints *ccs,
																		int32 chunk_id,
																		const char chunk_relkind,
																		Oid hypertable_oid);
extern TSDLLEXPORT int ts_chunk_constraints_add_inheritable_check_constraints(
	ChunkConstraints *ccs, int32 chunk_id, const char chunk_relkind, Oid hypertable_oid);
extern TSDLLEXPORT void ts_chunk_constraints_insert_metadata(const ChunkConstraints *ccs);
extern TSDLLEXPORT void ts_chunk_constraints_create(const Hypertable *ht, const Chunk *chunk);
extern void ts_chunk_constraint_create_on_chunk(const Hypertable *ht, const Chunk *chunk,
												Oid constraint_oid);
extern int ts_chunk_constraint_delete_by_hypertable_constraint_name(
	int32 chunk_id, const char *hypertable_constraint_name, bool delete_metadata,
	bool drop_constraint);
extern int ts_chunk_constraint_delete_by_chunk_id(int32 chunk_id, ChunkConstraints *ccs);
extern int ts_chunk_constraint_delete_by_dimension_slice_id(int32 dimension_slice_id);
extern int ts_chunk_constraint_delete_by_constraint_name(int32 chunk_id,
														 const char *constraint_name,
														 bool delete_metadata,
														 bool drop_constraint);
extern void ts_chunk_constraints_recreate(const Hypertable *ht, const Chunk *chunk);
extern int ts_chunk_constraint_rename_hypertable_constraint(int32 chunk_id, const char *old_name,
															const char *new_name);
extern int ts_chunk_constraint_adjust_meta(int32 chunk_id, const char *ht_constraint_name,
										   const char *old_name, const char *new_name);
extern TSDLLEXPORT bool ts_chunk_constraint_update_slice_id(int32 chunk_id, int32 old_slice_id,
															int32 new_slice_id);

extern char *
ts_chunk_constraint_get_name_from_hypertable_constraint(Oid chunk_relid,
														const char *hypertable_constraint_name);
extern void ts_chunk_constraint_insert(ChunkConstraint *constraint);
extern ChunkConstraint *ts_chunk_constraints_add_from_tuple(ChunkConstraints *ccs,
															const TupleInfo *ti);

extern ScanIterator ts_chunk_constraint_scan_iterator_create(MemoryContext result_mcxt);
extern void ts_chunk_constraint_scan_iterator_set_slice_id(ScanIterator *it, int32 slice_id);
extern void ts_chunk_constraint_scan_iterator_set_chunk_id(ScanIterator *it, int32 chunk_id);
