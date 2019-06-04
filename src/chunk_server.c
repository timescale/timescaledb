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

#include "chunk_server.h"
#include "scanner.h"
#include "chunk.h"

static void
chunk_server_insert_relation(Relation rel, int32 chunk_id, int32 server_chunk_id, Name server_name)
{
	TupleDesc desc = RelationGetDescr(rel);
	Datum values[Natts_chunk_server];
	bool nulls[Natts_chunk_server] = { false };
	CatalogSecurityContext sec_ctx;

	values[AttrNumberGetAttrOffset(Anum_chunk_server_chunk_id)] = Int32GetDatum(chunk_id);
	values[AttrNumberGetAttrOffset(Anum_chunk_server_server_chunk_id)] =
		Int32GetDatum(server_chunk_id);
	values[AttrNumberGetAttrOffset(Anum_chunk_server_server_name)] = NameGetDatum(server_name);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert_values(rel, desc, values, nulls);
	ts_catalog_restore_user(&sec_ctx);
}

static void
chunk_server_insert_internal(int32 chunk_id, int32 server_chunk_id, Name server_name)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel;

	rel = heap_open(catalog->tables[CHUNK_SERVER].id, RowExclusiveLock);

	chunk_server_insert_relation(rel, chunk_id, server_chunk_id, server_name);

	heap_close(rel, RowExclusiveLock);
}

void
ts_chunk_server_insert(ChunkServer *server)
{
	chunk_server_insert_internal(server->fd.chunk_id,
								 server->fd.server_chunk_id,
								 &server->fd.server_name);
}

void
ts_chunk_server_insert_multi(List *chunk_servers)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel;
	ListCell *lc;

	rel = heap_open(catalog->tables[CHUNK_SERVER].id, RowExclusiveLock);

	foreach (lc, chunk_servers)
	{
		ChunkServer *server = lfirst(lc);

		chunk_server_insert_relation(rel,
									 server->fd.chunk_id,
									 server->fd.server_chunk_id,
									 &server->fd.server_name);
	}

	heap_close(rel, RowExclusiveLock);
}

static int
chunk_server_scan_limit_internal(ScanKeyData *scankey, int num_scankeys, int indexid,
								 tuple_found_func on_tuple_found, void *scandata, int limit,
								 LOCKMODE lock, MemoryContext mctx)
{
	Catalog *catalog = ts_catalog_get();
	ScannerCtx scanctx = {
		.table = catalog->tables[CHUNK_SERVER].id,
		.index = catalog_get_index(catalog, CHUNK_SERVER, indexid),
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
chunk_server_tuple_found(TupleInfo *ti, void *data)
{
	List **servers = data;
	Form_chunk_server form = (Form_chunk_server) GETSTRUCT(ti->tuple);
	ChunkServer *chunk_server;
	ForeignServer *foreign_server = GetForeignServerByName(NameStr(form->server_name), false);
	MemoryContext old;

	old = MemoryContextSwitchTo(ti->mctx);
	chunk_server = palloc(sizeof(ChunkServer));
	memcpy(&chunk_server->fd, form, sizeof(FormData_chunk_server));
	chunk_server->foreign_server_oid = foreign_server->serverid;
	*servers = lappend(*servers, chunk_server);
	MemoryContextSwitchTo(old);

	return SCAN_CONTINUE;
}

static int
ts_chunk_server_scan_by_chunk_id_and_server_internal(int32 chunk_id, const char *servername,
													 tuple_found_func tuple_found, void *data,
													 LOCKMODE lockmode, MemoryContext mctx)
{
	ScanKeyData scankey[2];
	int nkeys = 0;

	ScanKeyInit(&scankey[nkeys++],
				Anum_chunk_server_chunk_id_server_name_idx_chunk_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(chunk_id));

	if (NULL != servername)
		ScanKeyInit(&scankey[nkeys++],
					Anum_chunk_server_chunk_id_server_name_idx_server_name,
					BTEqualStrategyNumber,
					F_NAMEEQ,
					DirectFunctionCall1(namein, CStringGetDatum(servername)));

	return chunk_server_scan_limit_internal(scankey,
											nkeys,
											CHUNK_SERVER_CHUNK_ID_SERVER_NAME_IDX,
											tuple_found,
											data,
											0,
											lockmode,
											mctx);
}

static int
ts_chunk_server_scan_by_server_internal(const char *servername, tuple_found_func tuple_found,
										void *data, LOCKMODE lockmode, MemoryContext mctx)
{
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_chunk_server_server_name,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				DirectFunctionCall1(namein, CStringGetDatum(servername)));

	return chunk_server_scan_limit_internal(scankey,
											1,
											INVALID_INDEXID,
											tuple_found,
											data,
											0,
											lockmode,
											mctx);
}

List *
ts_chunk_server_scan_by_chunk_id(int32 chunk_id, MemoryContext mctx)
{
	List *chunk_servers = NIL;

	ts_chunk_server_scan_by_chunk_id_and_server_internal(chunk_id,
														 NULL,
														 chunk_server_tuple_found,
														 &chunk_servers,
														 AccessShareLock,
														 mctx);
	return chunk_servers;
}

ChunkServer *
ts_chunk_server_scan_by_chunk_id_and_servername(int32 chunk_id, const char *servername,
												MemoryContext mctx)

{
	List *chunk_servers = NIL;

	ts_chunk_server_scan_by_chunk_id_and_server_internal(chunk_id,
														 servername,
														 chunk_server_tuple_found,
														 &chunk_servers,
														 AccessShareLock,
														 mctx);
	Assert(list_length(chunk_servers) <= 1);

	if (chunk_servers == NIL)
		return NULL;
	return linitial(chunk_servers);
}

static ScanTupleResult
chunk_server_tuple_delete(TupleInfo *ti, void *data)
{
	CatalogSecurityContext sec_ctx;

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_delete(ti->scanrel, ti->tuple);
	ts_catalog_restore_user(&sec_ctx);

	return SCAN_CONTINUE;
}

int
ts_chunk_server_delete_by_chunk_id(int32 chunk_id)
{
	return ts_chunk_server_scan_by_chunk_id_and_server_internal(chunk_id,
																NULL,
																chunk_server_tuple_delete,
																NULL,
																RowExclusiveLock,
																CurrentMemoryContext);
}

TSDLLEXPORT int
ts_chunk_server_delete_by_chunk_id_and_server_name(int32 chunk_id, const char *server_name)
{
	return ts_chunk_server_scan_by_chunk_id_and_server_internal(chunk_id,
																server_name,
																chunk_server_tuple_delete,
																NULL,
																RowExclusiveLock,
																CurrentMemoryContext);
}

int
ts_chunk_server_delete_by_servername(const char *servername)
{
	return ts_chunk_server_scan_by_server_internal(servername,
												   chunk_server_tuple_delete,
												   NULL,
												   RowExclusiveLock,
												   CurrentMemoryContext);
}

TSDLLEXPORT List *
ts_chunk_server_scan_by_servername_and_hypertable_id(const char *server_name, int32 hypertable_id,
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
		ChunkServer *cs =
			ts_chunk_server_scan_by_chunk_id_and_servername(chunk_id, server_name, mctx);
		if (cs != NULL)
			results = lappend(results, cs);
	}

	MemoryContextSwitchTo(old);
	return results;
}

TSDLLEXPORT bool
ts_chunk_server_contains_non_replicated_chunks(List *chunk_servers)
{
	ListCell *lc;

	foreach (lc, chunk_servers)
	{
		ChunkServer *cs = lfirst(lc);
		List *replicas = ts_chunk_server_scan_by_chunk_id(cs->fd.chunk_id, CurrentMemoryContext);
		if (list_length(replicas) < 2)
			return true;
	}

	return false;
}

TSDLLEXPORT void
ts_chunk_server_update_foreign_table_server(Oid relid, Oid new_server_id)
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
ts_chunk_server_update_foreign_table_server_if_needed(int32 chunk_id, Oid existing_server_id)
{
	ListCell *lc;
	ChunkServer *new_server = NULL;
	Chunk *chunk = ts_chunk_get_by_id(chunk_id, 0, true);
	ForeignTable *foreign_table = NULL;

	Assert(chunk->relkind == RELKIND_FOREIGN_TABLE);
	foreign_table = GetForeignTable(chunk->table_id);

	/* no need to update since foreign table doesn't reference server we try to remove */
	if (existing_server_id != foreign_table->serverid)
		return;

	Assert(list_length(chunk->servers) > 1);

	foreach (lc, chunk->servers)
	{
		new_server = lfirst(lc);
		if (new_server->foreign_server_oid != existing_server_id)
			break;
	}
	Assert(new_server != NULL);
	ts_chunk_server_update_foreign_table_server(chunk->table_id, new_server->foreign_server_oid);
}
