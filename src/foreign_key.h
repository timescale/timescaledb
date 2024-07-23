/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <catalog/pg_constraint.h>
#include <nodes/parsenodes.h>

#include "chunk.h"
#include "export.h"
#include "hypertable.h"

extern TSDLLEXPORT void ts_fk_propagate(Oid conrelid, Hypertable *ht);
extern TSDLLEXPORT void ts_chunk_copy_referencing_fk(const Hypertable *ht, const Chunk *chunk);
