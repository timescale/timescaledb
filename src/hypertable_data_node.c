/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <utils/fmgroids.h>
#include <utils/builtins.h>
#include <utils/array.h>
#include <foreign/foreign.h>

#include "hypertable_data_node.h"
#include "scanner.h"
#include "catalog.h"

static void
hypertable_data_node_insert_relation(Relation rel, int32 hypertable_id, int32 node_hypertable_id,
									 Name node_name, bool block_chunks)
{
	TupleDesc desc = RelationGetDescr(rel);
	Datum values[Natts_hypertable_data_node];
	bool nulls[Natts_hypertable_data_node] = { false };
	CatalogSecurityContext sec_ctx;

	values[AttrNumberGetAttrOffset(Anum_hypertable_data_node_hypertable_id)] =
		Int32GetDatum(hypertable_id);
	values[AttrNumberGetAttrOffset(Anum_hypertable_data_node_node_name)] = NameGetDatum(node_name);
	values[AttrNumberGetAttrOffset(Anum_hypertable_data_node_block_chunks)] = block_chunks;

	if (node_hypertable_id > 0)
		values[AttrNumberGetAttrOffset(Anum_hypertable_data_node_node_hypertable_id)] =
			Int32GetDatum(node_hypertable_id);
	else
		nulls[AttrNumberGetAttrOffset(Anum_hypertable_data_node_node_hypertable_id)] = true;

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert_values(rel, desc, values, nulls);
	ts_catalog_restore_user(&sec_ctx);
}

TSDLLEXPORT void
ts_hypertable_data_node_insert_multi(List *hypertable_data_nodes)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel;
	ListCell *lc;

	rel = heap_open(catalog->tables[HYPERTABLE_DATA_NODE].id, RowExclusiveLock);

	foreach (lc, hypertable_data_nodes)
	{
		HypertableDataNode *node = lfirst(lc);

		hypertable_data_node_insert_relation(rel,
											 node->fd.hypertable_id,
											 node->fd.node_hypertable_id,
											 &node->fd.node_name,
											 node->fd.block_chunks);
	}

	heap_close(rel, RowExclusiveLock);
}

static int
hypertable_data_node_scan_limit_internal(ScanKeyData *scankey, int num_scankeys, int indexid,
										 tuple_found_func on_tuple_found, void *scandata, int limit,
										 LOCKMODE lock, MemoryContext mctx)
{
	Catalog *catalog = ts_catalog_get();
	ScannerCtx scanctx = {
		.table = catalog->tables[HYPERTABLE_DATA_NODE].id,
		.index = catalog_get_index(catalog, HYPERTABLE_DATA_NODE, indexid),
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
hypertable_data_node_tuple_delete(TupleInfo *ti, void *data)
{
	CatalogSecurityContext sec_ctx;

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_delete(ti->scanrel, ti->tuple);
	ts_catalog_restore_user(&sec_ctx);

	return SCAN_CONTINUE;
}

static ScanTupleResult
hypertable_data_node_tuple_update(TupleInfo *ti, void *data)
{
	CatalogSecurityContext sec_ctx;
	HypertableDataNode *update = data;
	HeapTuple tuple = heap_copytuple(ti->tuple);
	FormData_hypertable_data_node *form = (FormData_hypertable_data_node *) GETSTRUCT(tuple);

	Assert(form->hypertable_id == update->fd.hypertable_id);
	Assert(strcmp(NameStr(form->node_name), NameStr(update->fd.node_name)) == 0);

	form->node_hypertable_id = update->fd.node_hypertable_id;
	form->block_chunks = update->fd.block_chunks;

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_update(ti->scanrel, tuple);
	ts_catalog_restore_user(&sec_ctx);

	return SCAN_DONE;
}

static HypertableDataNode *
hypertable_data_node_create_from_tuple(TupleInfo *ti)
{
	const char *node_name;
	HypertableDataNode *hypertable_data_node;
	ForeignServer *foreign_server;
	MemoryContext old;
	Datum values[Natts_hypertable_data_node];
	bool nulls[Natts_hypertable_data_node] = { false };

	/*
	 * Need to use heap_deform_tuple instead of GETSTRUCT since the tuple can
	 * contain NULL values
	 */
	heap_deform_tuple(ti->tuple, ti->desc, values, nulls);

	node_name =
		DatumGetCString(values[AttrNumberGetAttrOffset(Anum_hypertable_data_node_node_name)]);
	foreign_server = GetForeignServerByName(node_name, false);

	old = MemoryContextSwitchTo(ti->mctx);
	hypertable_data_node = palloc(sizeof(HypertableDataNode));

	hypertable_data_node->fd.hypertable_id =
		DatumGetInt32(values[AttrNumberGetAttrOffset(Anum_hypertable_data_node_hypertable_id)]);
	namestrcpy(&hypertable_data_node->fd.node_name, node_name);

	if (nulls[AttrNumberGetAttrOffset(Anum_hypertable_data_node_node_hypertable_id)])
		hypertable_data_node->fd.node_hypertable_id = 0;
	else
		hypertable_data_node->fd.node_hypertable_id = DatumGetInt32(
			values[AttrNumberGetAttrOffset(Anum_hypertable_data_node_node_hypertable_id)]);

	hypertable_data_node->fd.block_chunks =
		DatumGetBool(values[AttrNumberGetAttrOffset(Anum_hypertable_data_node_block_chunks)]);
	hypertable_data_node->foreign_server_oid = foreign_server->serverid;
	MemoryContextSwitchTo(old);

	return hypertable_data_node;
}

static ScanTupleResult
hypertable_data_node_tuples_found(TupleInfo *ti, void *data)
{
	List **nodes = data;
	MemoryContext old;
	HypertableDataNode *hypertable_data_node = hypertable_data_node_create_from_tuple(ti);

	old = MemoryContextSwitchTo(ti->mctx);
	*nodes = lappend(*nodes, hypertable_data_node);
	MemoryContextSwitchTo(old);

	return SCAN_CONTINUE;
}

static int
hypertable_data_node_scan_by_hypertable_id(int32 hypertable_id, tuple_found_func tuple_found,
										   void *data, LOCKMODE lockmode, MemoryContext mctx)
{
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_hypertable_data_node_hypertable_id_node_name_idx_hypertable_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(hypertable_id));

	return hypertable_data_node_scan_limit_internal(
		scankey,
		1,
		HYPERTABLE_DATA_NODE_HYPERTABLE_ID_NODE_NAME_IDX,
		tuple_found,
		data,
		0,
		lockmode,
		mctx);
}

static int
hypertable_data_node_scan_by_node_name(const char *node_name, tuple_found_func tuple_found,
									   void *data, LOCKMODE lockmode, MemoryContext mctx)
{
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_hypertable_data_node_node_name,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				DirectFunctionCall1(namein, CStringGetDatum(node_name)));

	return hypertable_data_node_scan_limit_internal(scankey,
													1,
													INVALID_INDEXID,
													tuple_found,
													data,
													0,
													lockmode,
													mctx);
}

static int
hypertable_data_node_scan_by_hypertable_id_and_node_name(int hypertable_id, const char *node_name,
														 tuple_found_func tuple_found, void *data,
														 LOCKMODE lockmode, MemoryContext mctx)
{
	ScanKeyData scankey[2];

	ScanKeyInit(&scankey[0],
				Anum_hypertable_data_node_hypertable_id_node_name_idx_hypertable_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(hypertable_id));

	ScanKeyInit(&scankey[1],
				Anum_hypertable_data_node_hypertable_id_node_name_idx_node_name,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				DirectFunctionCall1(namein, CStringGetDatum(node_name)));

	return hypertable_data_node_scan_limit_internal(
		scankey,
		2,
		HYPERTABLE_DATA_NODE_HYPERTABLE_ID_NODE_NAME_IDX,
		tuple_found,
		data,
		0,
		lockmode,
		mctx);
}

TSDLLEXPORT List *
ts_hypertable_data_node_scan(int32 hypertable_id, MemoryContext mctx)
{
	List *hypertable_data_nodes = NIL;

	hypertable_data_node_scan_by_hypertable_id(hypertable_id,
											   hypertable_data_node_tuples_found,
											   &hypertable_data_nodes,
											   AccessShareLock,
											   mctx);

	return hypertable_data_nodes;
}

TSDLLEXPORT int
ts_hypertable_data_node_delete_by_hypertable_id(int32 hypertable_id)
{
	return hypertable_data_node_scan_by_hypertable_id(hypertable_id,
													  hypertable_data_node_tuple_delete,
													  NULL,
													  RowExclusiveLock,
													  CurrentMemoryContext);
}

TSDLLEXPORT int
ts_hypertable_data_node_delete_by_node_name(const char *node_name)
{
	return hypertable_data_node_scan_by_node_name(node_name,
												  hypertable_data_node_tuple_delete,
												  NULL,
												  RowExclusiveLock,
												  CurrentMemoryContext);
}

TSDLLEXPORT int
ts_hypertable_data_node_delete_by_node_name_and_hypertable_id(const char *node_name,
															  int32 hypertable_id)
{
	return hypertable_data_node_scan_by_hypertable_id_and_node_name(
		hypertable_id,
		node_name,
		hypertable_data_node_tuple_delete,
		NULL,
		RowExclusiveLock,
		CurrentMemoryContext);
}

List *
ts_hypertable_data_node_scan_by_node_name(const char *node_name, MemoryContext mctx)
{
	List *hypertable_data_nodes = NIL;

	hypertable_data_node_scan_by_node_name(node_name,
										   hypertable_data_node_tuples_found,
										   &hypertable_data_nodes,
										   AccessShareLock,
										   mctx);
	return hypertable_data_nodes;
}

TSDLLEXPORT int
ts_hypertable_data_node_update(HypertableDataNode *hypertable_data_node)
{
	return hypertable_data_node_scan_by_hypertable_id_and_node_name(
		hypertable_data_node->fd.hypertable_id,
		NameStr(hypertable_data_node->fd.node_name),
		hypertable_data_node_tuple_update,
		hypertable_data_node,
		RowExclusiveLock,
		CurrentMemoryContext);
}
