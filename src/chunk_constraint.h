/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <nodes/parsenodes.h>

#include "hypertable.h"

typedef struct Chunk Chunk;
typedef struct DimensionSlice DimensionSlice;
typedef struct Dimension Dimension;

extern void ts_chunk_clone_check_constraints(Oid chunk_relid, Oid hypertable_oid);
extern TSDLLEXPORT Constraint *ts_chunk_constraint_dimensional_create(const Dimension *dim,
																	  const DimensionSlice *slice,
																	  const char *name);
extern TSDLLEXPORT void ts_chunk_constraints_create(const Hypertable *ht, const Chunk *chunk);
extern void ts_chunk_constraint_create_on_chunk(const Hypertable *ht, const Chunk *chunk,
												Oid constraint_oid);
extern void ts_chunk_constraints_recreate(const Hypertable *ht, const Chunk *chunk);

extern char *
ts_chunk_constraint_get_name_from_hypertable_constraint(Oid chunk_relid,
														const char *hypertable_constraint_name);
extern TSDLLEXPORT void ts_chunk_constraint_choose_name(char *dst, int32 chunk_id,
														const char *hypertable_constraint_name);

extern TSDLLEXPORT void ts_chunk_constraint_check_violated(const Chunk *chunk,
														   const Hyperspace *hs);
