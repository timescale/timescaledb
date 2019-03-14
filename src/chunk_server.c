/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <foreign/foreign.h>
#include <utils/builtins.h>

#include "chunk_server.h"
#include "scanner.h"

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
	Assert(list_length(chunk_servers) == 1);

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

int
ts_chunk_server_delete_by_servername(const char *servername)
{
	return ts_chunk_server_scan_by_server_internal(servername,
												   chunk_server_tuple_delete,
												   NULL,
												   RowExclusiveLock,
												   CurrentMemoryContext);
}
