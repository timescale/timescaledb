/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_foreign_table.h>
#include <catalog/pg_foreign_server.h>
#include <catalog/dependency.h>
#include <foreign/foreign.h>
#include <utils/builtins.h>
#include <utils/syscache.h>
#include <utils/inval.h>
#include <access/xact.h>

#include "ts_catalog/chunk_data_node.h"
#include "scanner.h"
#include "chunk.h"

static void
chunk_data_node_insert_relation(const Relation rel, int32 chunk_id, int32 node_chunk_id,
								const NameData *node_name)
{
	TupleDesc desc = RelationGetDescr(rel);
	Datum values[Natts_chunk_data_node];
	bool nulls[Natts_chunk_data_node] = { false };
	CatalogSecurityContext sec_ctx;

	values[AttrNumberGetAttrOffset(Anum_chunk_data_node_chunk_id)] = Int32GetDatum(chunk_id);
	values[AttrNumberGetAttrOffset(Anum_chunk_data_node_node_chunk_id)] =
		Int32GetDatum(node_chunk_id);
	values[AttrNumberGetAttrOffset(Anum_chunk_data_node_node_name)] = NameGetDatum(node_name);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert_values(rel, desc, values, nulls);
	ts_catalog_restore_user(&sec_ctx);
}

static void
chunk_data_node_insert_internal(int32 chunk_id, int32 node_chunk_id, const NameData *node_name)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel;

	rel = table_open(catalog->tables[CHUNK_DATA_NODE].id, RowExclusiveLock);

	chunk_data_node_insert_relation(rel, chunk_id, node_chunk_id, node_name);

	table_close(rel, RowExclusiveLock);
}

void
ts_chunk_data_node_insert(const ChunkDataNode *node)
{
	chunk_data_node_insert_internal(node->fd.chunk_id, node->fd.node_chunk_id, &node->fd.node_name);
}

void
ts_chunk_data_node_insert_multi(List *chunk_data_nodes)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel;
	ListCell *lc;

	rel = table_open(catalog->tables[CHUNK_DATA_NODE].id, RowExclusiveLock);

	foreach (lc, chunk_data_nodes)
	{
		ChunkDataNode *node = lfirst(lc);

		chunk_data_node_insert_relation(rel,
										node->fd.chunk_id,
										node->fd.node_chunk_id,
										&node->fd.node_name);
	}

	table_close(rel, RowExclusiveLock);
}

static int
chunk_data_node_scan_limit_internal(ScanKeyData *scankey, int num_scankeys, int indexid,
									tuple_found_func on_tuple_found, void *scandata, int limit,
									LOCKMODE lock, MemoryContext mctx)
{
	Catalog *catalog = ts_catalog_get();
	ScannerCtx scanctx = {
		.table = catalog->tables[CHUNK_DATA_NODE].id,
		.index = catalog_get_index(catalog, CHUNK_DATA_NODE, indexid),
		.nkeys = num_scankeys,
		.scankey = scankey,
		.data = scandata,
		.limit = limit,
		.tuple_found = on_tuple_found,
		.lockmode = lock,
		.scandirection = ForwardScanDirection,
		.result_mctx = mctx,
	};

	return ts_scanner_scan(&scanctx);
}

static ScanTupleResult
chunk_data_node_tuple_found(TupleInfo *ti, void *data)
{
	List **nodes = data;
	bool should_free;
	HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
	Form_chunk_data_node form = (Form_chunk_data_node) GETSTRUCT(tuple);
	ChunkDataNode *chunk_data_node;
	MemoryContext old;

	old = MemoryContextSwitchTo(ti->mctx);
	chunk_data_node = palloc(sizeof(ChunkDataNode));
	memcpy(&chunk_data_node->fd, form, sizeof(FormData_chunk_data_node));
	chunk_data_node->foreign_server_oid =
		get_foreign_server_oid(NameStr(form->node_name), /* missing_ok = */ false);
	*nodes = lappend(*nodes, chunk_data_node);
	MemoryContextSwitchTo(old);

	if (should_free)
		heap_freetuple(tuple);

	return SCAN_CONTINUE;
}

static int
ts_chunk_data_node_scan_by_chunk_id_and_node_internal(int32 chunk_id, const char *node_name,
													  bool scan_by_remote_chunk_id,
													  tuple_found_func tuple_found, void *data,
													  LOCKMODE lockmode, MemoryContext mctx)
{
	ScanKeyData scankey[2];
	int nkeys = 0;
	int attrnum_chunk_id;
	int attrnum_node_name;
	int indexid;

	if (scan_by_remote_chunk_id)
	{
		attrnum_chunk_id = Anum_chunk_data_node_node_chunk_id_node_name_idx_chunk_id;
		attrnum_node_name = Anum_chunk_data_node_node_chunk_id_node_name_idx_node_name;
		indexid = CHUNK_DATA_NODE_NODE_CHUNK_ID_NODE_NAME_IDX;
	}
	else
	{
		attrnum_chunk_id = Anum_chunk_data_node_chunk_id_node_name_idx_chunk_id;
		attrnum_node_name = Anum_chunk_data_node_chunk_id_node_name_idx_node_name;
		indexid = CHUNK_DATA_NODE_CHUNK_ID_NODE_NAME_IDX;
	}

	ScanKeyInit(&scankey[nkeys++],
				attrnum_chunk_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(chunk_id));

	if (NULL != node_name)
		ScanKeyInit(&scankey[nkeys++],
					attrnum_node_name,
					BTEqualStrategyNumber,
					F_NAMEEQ,
					CStringGetDatum(node_name));

	return chunk_data_node_scan_limit_internal(scankey,
											   nkeys,
											   indexid,
											   tuple_found,
											   data,
											   0,
											   lockmode,
											   mctx);
}

static int
ts_chunk_data_node_scan_by_node_internal(const char *node_name, tuple_found_func tuple_found,
										 void *data, LOCKMODE lockmode, MemoryContext mctx)
{
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_chunk_data_node_node_name,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				CStringGetDatum(node_name));

	return chunk_data_node_scan_limit_internal(scankey,
											   1,
											   INVALID_INDEXID,
											   tuple_found,
											   data,
											   0,
											   lockmode,
											   mctx);
}

/* Returns a List of ChunkDataNode structs. */
List *
ts_chunk_data_node_scan_by_chunk_id(int32 chunk_id, MemoryContext mctx)
{
	List *chunk_data_nodes = NIL;

	ts_chunk_data_node_scan_by_chunk_id_and_node_internal(chunk_id,
														  NULL,
														  false,
														  chunk_data_node_tuple_found,
														  &chunk_data_nodes,
														  AccessShareLock,
														  mctx);
	return chunk_data_nodes;
}

static ChunkDataNode *
chunk_data_node_scan_by_chunk_id_and_node_name(int32 chunk_id, const char *node_name,
											   bool scan_by_remote_chunk_id, MemoryContext mctx)

{
	List *chunk_data_nodes = NIL;

	ts_chunk_data_node_scan_by_chunk_id_and_node_internal(chunk_id,
														  node_name,
														  scan_by_remote_chunk_id,
														  chunk_data_node_tuple_found,
														  &chunk_data_nodes,
														  AccessShareLock,
														  mctx);
	Assert(list_length(chunk_data_nodes) <= 1);

	if (chunk_data_nodes == NIL)
		return NULL;
	return linitial(chunk_data_nodes);
}

ChunkDataNode *
ts_chunk_data_node_scan_by_chunk_id_and_node_name(int32 chunk_id, const char *node_name,
												  MemoryContext mctx)

{
	return chunk_data_node_scan_by_chunk_id_and_node_name(chunk_id, node_name, false, mctx);
}

ChunkDataNode *
ts_chunk_data_node_scan_by_remote_chunk_id_and_node_name(int32 chunk_id, const char *node_name,
														 MemoryContext mctx)

{
	return chunk_data_node_scan_by_chunk_id_and_node_name(chunk_id, node_name, true, mctx);
}

static ScanTupleResult
chunk_data_node_tuple_delete(TupleInfo *ti, void *data)
{
	CatalogSecurityContext sec_ctx;

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));
	ts_catalog_restore_user(&sec_ctx);

	return SCAN_CONTINUE;
}

int
ts_chunk_data_node_delete_by_chunk_id(int32 chunk_id)
{
	return ts_chunk_data_node_scan_by_chunk_id_and_node_internal(chunk_id,
																 NULL,
																 false,
																 chunk_data_node_tuple_delete,
																 NULL,
																 RowExclusiveLock,
																 CurrentMemoryContext);
}

int
ts_chunk_data_node_delete_by_chunk_id_and_node_name(int32 chunk_id, const char *node_name)
{
	return ts_chunk_data_node_scan_by_chunk_id_and_node_internal(chunk_id,
																 node_name,
																 false,
																 chunk_data_node_tuple_delete,
																 NULL,
																 RowExclusiveLock,
																 CurrentMemoryContext);
}

int
ts_chunk_data_node_delete_by_node_name(const char *node_name)
{
	return ts_chunk_data_node_scan_by_node_internal(node_name,
													chunk_data_node_tuple_delete,
													NULL,
													RowExclusiveLock,
													CurrentMemoryContext);
}

List *
ts_chunk_data_node_scan_by_node_name_and_hypertable_id(const char *node_name, int32 hypertable_id,
													   MemoryContext mctx)
{
	List *results = NIL;
	ListCell *lc;
	MemoryContext old;
	List *chunk_ids = NIL;

	old = MemoryContextSwitchTo(mctx);
	chunk_ids = ts_chunk_get_chunk_ids_by_hypertable_id(hypertable_id);

	foreach (lc, chunk_ids)
	{
		int32 chunk_id = lfirst_int(lc);
		ChunkDataNode *cdn =
			ts_chunk_data_node_scan_by_chunk_id_and_node_name(chunk_id, node_name, mctx);
		if (cdn != NULL)
			results = lappend(results, cdn);
	}

	MemoryContextSwitchTo(old);
	return results;
}

ScanIterator
ts_chunk_data_nodes_scan_iterator_create(MemoryContext result_mcxt)
{
	ScanIterator it = ts_scan_iterator_create(CHUNK_DATA_NODE, AccessShareLock, result_mcxt);
	it.ctx.flags |= SCANNER_F_NOEND_AND_NOCLOSE;

	return it;
}

void
ts_chunk_data_nodes_scan_iterator_set_chunk_id(ScanIterator *it, int32 chunk_id)
{
	it->ctx.index = catalog_get_index(ts_catalog_get(),
									  CHUNK_DATA_NODE,
									  CHUNK_DATA_NODE_CHUNK_ID_NODE_NAME_IDX);
	ts_scan_iterator_scan_key_reset(it);
	ts_scan_iterator_scan_key_init(it,
								   Anum_chunk_data_node_chunk_id_node_name_idx_chunk_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(chunk_id));
}
