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

#include "chunk_data_node.h"
#include "scanner.h"
#include "chunk.h"

static void
chunk_data_node_insert_relation(Relation rel, int32 chunk_id, int32 node_chunk_id, Name node_name)
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
chunk_data_node_insert_internal(int32 chunk_id, int32 node_chunk_id, Name node_name)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel;

	rel = heap_open(catalog->tables[CHUNK_DATA_NODE].id, RowExclusiveLock);

	chunk_data_node_insert_relation(rel, chunk_id, node_chunk_id, node_name);

	heap_close(rel, RowExclusiveLock);
}

void
ts_chunk_data_node_insert(ChunkDataNode *node)
{
	chunk_data_node_insert_internal(node->fd.chunk_id, node->fd.node_chunk_id, &node->fd.node_name);
}

void
ts_chunk_data_node_insert_multi(List *chunk_data_nodes)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel;
	ListCell *lc;

	rel = heap_open(catalog->tables[CHUNK_DATA_NODE].id, RowExclusiveLock);

	foreach (lc, chunk_data_nodes)
	{
		ChunkDataNode *node = lfirst(lc);

		chunk_data_node_insert_relation(rel,
										node->fd.chunk_id,
										node->fd.node_chunk_id,
										&node->fd.node_name);
	}

	heap_close(rel, RowExclusiveLock);
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
	Form_chunk_data_node form = (Form_chunk_data_node) GETSTRUCT(ti->tuple);
	ChunkDataNode *chunk_data_node;
	ForeignServer *foreign_server = GetForeignServerByName(NameStr(form->node_name), false);
	MemoryContext old;

	old = MemoryContextSwitchTo(ti->mctx);
	chunk_data_node = palloc(sizeof(ChunkDataNode));
	memcpy(&chunk_data_node->fd, form, sizeof(FormData_chunk_data_node));
	chunk_data_node->foreign_server_oid = foreign_server->serverid;
	*nodes = lappend(*nodes, chunk_data_node);
	MemoryContextSwitchTo(old);

	return SCAN_CONTINUE;
}

static int
ts_chunk_data_node_scan_by_chunk_id_and_node_internal(int32 chunk_id, const char *node_name,
													  tuple_found_func tuple_found, void *data,
													  LOCKMODE lockmode, MemoryContext mctx)
{
	ScanKeyData scankey[2];
	int nkeys = 0;

	ScanKeyInit(&scankey[nkeys++],
				Anum_chunk_data_node_chunk_id_node_name_idx_chunk_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(chunk_id));

	if (NULL != node_name)
		ScanKeyInit(&scankey[nkeys++],
					Anum_chunk_data_node_chunk_id_node_name_idx_node_name,
					BTEqualStrategyNumber,
					F_NAMEEQ,
					DirectFunctionCall1(namein, CStringGetDatum(node_name)));

	return chunk_data_node_scan_limit_internal(scankey,
											   nkeys,
											   CHUNK_DATA_NODE_CHUNK_ID_NODE_NAME_IDX,
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
				DirectFunctionCall1(namein, CStringGetDatum(node_name)));

	return chunk_data_node_scan_limit_internal(scankey,
											   1,
											   INVALID_INDEXID,
											   tuple_found,
											   data,
											   0,
											   lockmode,
											   mctx);
}

List *
ts_chunk_data_node_scan_by_chunk_id(int32 chunk_id, MemoryContext mctx)
{
	List *chunk_data_nodes = NIL;

	ts_chunk_data_node_scan_by_chunk_id_and_node_internal(chunk_id,
														  NULL,
														  chunk_data_node_tuple_found,
														  &chunk_data_nodes,
														  AccessShareLock,
														  mctx);
	return chunk_data_nodes;
}

ChunkDataNode *
ts_chunk_data_node_scan_by_chunk_id_and_node_name(int32 chunk_id, const char *node_name,
												  MemoryContext mctx)

{
	List *chunk_data_nodes = NIL;

	ts_chunk_data_node_scan_by_chunk_id_and_node_internal(chunk_id,
														  node_name,
														  chunk_data_node_tuple_found,
														  &chunk_data_nodes,
														  AccessShareLock,
														  mctx);
	Assert(list_length(chunk_data_nodes) <= 1);

	if (chunk_data_nodes == NIL)
		return NULL;
	return linitial(chunk_data_nodes);
}

static ScanTupleResult
chunk_data_node_tuple_delete(TupleInfo *ti, void *data)
{
	CatalogSecurityContext sec_ctx;

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_delete(ti->scanrel, ti->tuple);
	ts_catalog_restore_user(&sec_ctx);

	return SCAN_CONTINUE;
}

int
ts_chunk_data_node_delete_by_chunk_id(int32 chunk_id)
{
	return ts_chunk_data_node_scan_by_chunk_id_and_node_internal(chunk_id,
																 NULL,
																 chunk_data_node_tuple_delete,
																 NULL,
																 RowExclusiveLock,
																 CurrentMemoryContext);
}

TSDLLEXPORT int
ts_chunk_data_node_delete_by_chunk_id_and_node_name(int32 chunk_id, const char *node_name)
{
	return ts_chunk_data_node_scan_by_chunk_id_and_node_internal(chunk_id,
																 node_name,
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

TSDLLEXPORT List *
ts_chunk_data_node_scan_by_node_name_and_hypertable_id(const char *node_name, int32 hypertable_id,
													   MemoryContext mctx)
{
	List *results = NIL;
	ListCell *lc;
	MemoryContext old;
	List *chunk_ids = NIL;

	old = MemoryContextSwitchTo(mctx);
	chunk_ids = ts_chunk_find_chunk_ids_by_hypertable_id(hypertable_id);

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

TSDLLEXPORT bool
ts_chunk_data_node_contains_non_replicated_chunks(List *chunk_data_nodes)
{
	ListCell *lc;

	foreach (lc, chunk_data_nodes)
	{
		ChunkDataNode *cdn = lfirst(lc);
		List *replicas =
			ts_chunk_data_node_scan_by_chunk_id(cdn->fd.chunk_id, CurrentMemoryContext);
		if (list_length(replicas) < 2)
			return true;
	}

	return false;
}

TSDLLEXPORT void
ts_chunk_data_node_update_foreign_table_server(Oid relid, Oid new_server_id)
{
	Relation ftrel;
	HeapTuple tuple;
	HeapTuple copy;
	Datum values[Natts_pg_foreign_table];
	bool nulls[Natts_pg_foreign_table];
	CatalogSecurityContext sec_ctx;
	Oid old_server_id;
	long updated;

	tuple = SearchSysCache1(FOREIGNTABLEREL, ObjectIdGetDatum(relid));

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("relation with OID %u does not exist", relid)));

	ftrel = heap_open(ForeignTableRelationId, RowExclusiveLock);

	heap_deform_tuple(tuple, RelationGetDescr(ftrel), values, nulls);

	old_server_id =
		DatumGetObjectId(values[AttrNumberGetAttrOffset(Anum_pg_foreign_table_ftserver)]);

	values[AttrNumberGetAttrOffset(Anum_pg_foreign_table_ftserver)] =
		ObjectIdGetDatum(new_server_id);

	copy = heap_form_tuple(RelationGetDescr(ftrel), values, nulls);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_update_tid(ftrel, &tuple->t_self, copy);
	ts_catalog_restore_user(&sec_ctx);

	heap_close(ftrel, RowExclusiveLock);
	heap_freetuple(copy);
	ReleaseSysCache(tuple);
	/* invalidate foreign table cache */
	CacheInvalidateRelcacheByRelid(ForeignTableRelationId);
	/* update dependencies between foreign table and foreign server */
	updated = changeDependencyFor(RelationRelationId,
								  relid,
								  ForeignServerRelationId,
								  old_server_id,
								  new_server_id);
	if (updated != 1)
		elog(ERROR,
			 "failed while trying to update server for foreign table %s",
			 get_rel_name(relid));

	/* make changes visible */
	CommandCounterIncrement();
}

TSDLLEXPORT void
ts_chunk_data_node_update_foreign_table_server_if_needed(int32 chunk_id, Oid existing_server_id)
{
	ListCell *lc;
	ChunkDataNode *new_node = NULL;
	Chunk *chunk = ts_chunk_get_by_id(chunk_id, 0, true);
	ForeignTable *foreign_table = NULL;

	Assert(chunk->relkind == RELKIND_FOREIGN_TABLE);
	foreign_table = GetForeignTable(chunk->table_id);

	/* no need to update since foreign table doesn't reference server we try to remove */
	if (existing_server_id != foreign_table->serverid)
		return;

	Assert(list_length(chunk->data_nodes) > 1);

	foreach (lc, chunk->data_nodes)
	{
		new_node = lfirst(lc);
		if (new_node->foreign_server_oid != existing_server_id)
			break;
	}
	Assert(new_node != NULL);
	ts_chunk_data_node_update_foreign_table_server(chunk->table_id, new_node->foreign_server_oid);
}
