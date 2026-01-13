/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include "ts_catalog/catalog.h"

extern TSDLLEXPORT void ts_chunk_rewrite_add(Oid chunk_relid, Oid new_relid);
extern TSDLLEXPORT bool ts_chunk_rewrite_get_with_lock(Oid chunk_relid, Form_chunk_rewrite form,
													   ItemPointer tid);
extern TSDLLEXPORT void ts_chunk_rewrite_delete_by_tid(const ItemPointer tid);

typedef enum ChunkRewriteDeleteResult
{
	ChunkRewriteOngoing,
	ChunkRewriteEntryDeleted,
	ChunkRewriteEntryDeletedAndTableDropped,
	ChunkRewriteEntryDoesNotExist,
} ChunkRewriteDeleteResult;

extern TSDLLEXPORT ChunkRewriteDeleteResult ts_chunk_rewrite_delete(Oid chunk_relid,
																	bool conditional);
