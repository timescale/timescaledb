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
extern TSDLLEXPORT void ts_chunk_drop_referencing_fk_by_chunk_id(Oid chunk_id);
extern TSDLLEXPORT void ts_chunk_inherit_outbound_fk(const Hypertable *ht, const Chunk *chunk);
extern TSDLLEXPORT void ts_chunk_inherit_outbound_fk_by_oid(const Chunk *chunk, Oid parent_fk_oid);
extern TSDLLEXPORT Oid ts_chunk_find_outbound_fk_by_parent(Oid chunk_relid, Oid parent_fk_oid);
