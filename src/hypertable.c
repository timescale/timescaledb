/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>

#include <access/heapam.h>
#include <access/htup_details.h>
#include <access/relscan.h>
#include <catalog/indexing.h>
#include <catalog/namespace.h>
#include <catalog/pg_collation.h>
#include <catalog/pg_constraint.h>
#include <catalog/pg_database_d.h>
#include <catalog/pg_inherits.h>
#include <catalog/pg_namespace_d.h>
#include <catalog/pg_proc.h>
#include <catalog/pg_type.h>
#include <commands/dbcommands.h>
#include <commands/schemacmds.h>
#include <commands/tablecmds.h>
#include <commands/tablespace.h>
#include <commands/trigger.h>
#include <executor/spi.h>
#include <funcapi.h>
#include <lib/stringinfo.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <nodes/memnodes.h>
#include <nodes/parsenodes.h>
#include <nodes/value.h>
#include <parser/parse_coerce.h>
#include <parser/parse_func.h>
#include <storage/lmgr.h>
#include <utils/acl.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/snapmgr.h>
#include <utils/syscache.h>

#include "hypertable.h"

#include "compat/compat.h"
#include "bgw_policy/policy.h"
#include "chunk.h"
#include "chunk_adaptive.h"
#include "copy.h"
#include "cross_module_fn.h"
#include "debug_assert.h"
#include "dimension.h"
#include "dimension_slice.h"
#include "dimension_vector.h"
#include "error_utils.h"
#include "errors.h"
#include "extension.h"
#include "guc.h"
#include "hypercube.h"
#include "hypertable_cache.h"
#include "indexing.h"
#include "license_guc.h"
#include "osm_callbacks.h"
#include "partition_chunk.h"
#include "scan_iterator.h"
#include "scanner.h"
#include "subspace_store.h"
#include "trigger.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/chunk_column_stats.h"
#include "ts_catalog/compression_settings.h"
#include "ts_catalog/continuous_agg.h"
#include "ts_catalog/metadata.h"
#include "utils.h"

Oid
ts_rel_get_owner(Oid relid)
{
	HeapTuple tuple;
	Oid ownerid;

	if (!OidIsValid(relid))
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("invalid relation OID")));

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("relation with OID %u does not exist", relid)));

	ownerid = ((Form_pg_class) GETSTRUCT(tuple))->relowner;

	ReleaseSysCache(tuple);

	return ownerid;
}

bool
ts_hypertable_has_privs_of(Oid hypertable_oid, Oid userid)
{
	return has_privs_of_role(userid, ts_rel_get_owner(hypertable_oid));
}

/*
 * The error output for permission denied errors such as these changed in PG11,
 * it modifies places where relation is specified to note the specific object
 * type we note that permissions are denied for the hypertable for all PG
 * versions so that tests need not change due to one word changes in error
 * messages and because it is more clear this way.
 */
Oid
ts_hypertable_permissions_check(Oid hypertable_oid, Oid userid)
{
	Oid ownerid = ts_rel_get_owner(hypertable_oid);

	if (!has_privs_of_role(userid, ownerid))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be owner of hypertable \"%s\"", get_rel_name(hypertable_oid))));

	return ownerid;
}

void
ts_hypertable_permissions_check_by_id(int32 hypertable_id)
{
	Oid table_relid = ts_hypertable_id_to_relid(hypertable_id, false);
	ts_hypertable_permissions_check(table_relid, GetUserId());
}

static Oid
get_chunk_sizing_func_oid(const FormData_hypertable *fd)
{
	Oid argtype[] = { INT4OID, INT8OID, INT8OID };
	return LookupFuncName(list_make2(makeString((char *) NameStr(fd->chunk_sizing_func_schema)),
									 makeString((char *) NameStr(fd->chunk_sizing_func_name))),
						  sizeof(argtype) / sizeof(argtype[0]),
						  argtype,
						  false);
}

static HeapTuple
hypertable_formdata_make_tuple(const FormData_hypertable *fd, TupleDesc desc)
{
	Datum values[Natts_hypertable];
	bool nulls[Natts_hypertable] = { false };

	memset(values, 0, sizeof(Datum) * Natts_hypertable);

	values[AttrNumberGetAttrOffset(Anum_hypertable_id)] = Int32GetDatum(fd->id);
	values[AttrNumberGetAttrOffset(Anum_hypertable_schema_name)] = NameGetDatum(&fd->schema_name);
	values[AttrNumberGetAttrOffset(Anum_hypertable_table_name)] = NameGetDatum(&fd->table_name);
	values[AttrNumberGetAttrOffset(Anum_hypertable_associated_schema_name)] =
		NameGetDatum(&fd->associated_schema_name);
	Assert(&fd->associated_table_prefix != NULL);
	values[AttrNumberGetAttrOffset(Anum_hypertable_associated_table_prefix)] =
		NameGetDatum(&fd->associated_table_prefix);
	values[AttrNumberGetAttrOffset(Anum_hypertable_num_dimensions)] =
		Int16GetDatum(fd->num_dimensions);

	values[AttrNumberGetAttrOffset(Anum_hypertable_chunk_sizing_func_schema)] =
		NameGetDatum(&fd->chunk_sizing_func_schema);
	values[AttrNumberGetAttrOffset(Anum_hypertable_chunk_sizing_func_name)] =
		NameGetDatum(&fd->chunk_sizing_func_name);
	values[AttrNumberGetAttrOffset(Anum_hypertable_chunk_target_size)] =
		Int64GetDatum(fd->chunk_target_size);

	values[AttrNumberGetAttrOffset(Anum_hypertable_compression_state)] =
		Int16GetDatum(fd->compression_state);
	if (fd->compressed_hypertable_id == INVALID_HYPERTABLE_ID)
		nulls[AttrNumberGetAttrOffset(Anum_hypertable_compressed_hypertable_id)] = true;
	else
		values[AttrNumberGetAttrOffset(Anum_hypertable_compressed_hypertable_id)] =
			Int32GetDatum(fd->compressed_hypertable_id);
	values[AttrNumberGetAttrOffset(Anum_hypertable_status)] = Int32GetDatum(fd->status);

	return heap_form_tuple(desc, values, nulls);
}

void
ts_hypertable_formdata_fill(FormData_hypertable *fd, const TupleInfo *ti)
{
	bool nulls[Natts_hypertable];
	Datum values[Natts_hypertable];
	bool should_free;
	HeapTuple tuple;

	tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
	heap_deform_tuple(tuple, ts_scanner_get_tupledesc(ti), values, nulls);

	Assert(!nulls[AttrNumberGetAttrOffset(Anum_hypertable_id)]);
	Assert(!nulls[AttrNumberGetAttrOffset(Anum_hypertable_schema_name)]);
	Assert(!nulls[AttrNumberGetAttrOffset(Anum_hypertable_table_name)]);
	Assert(!nulls[AttrNumberGetAttrOffset(Anum_hypertable_associated_schema_name)]);
	Assert(!nulls[AttrNumberGetAttrOffset(Anum_hypertable_associated_table_prefix)]);
	Assert(!nulls[AttrNumberGetAttrOffset(Anum_hypertable_num_dimensions)]);
	Assert(!nulls[AttrNumberGetAttrOffset(Anum_hypertable_chunk_sizing_func_schema)]);
	Assert(!nulls[AttrNumberGetAttrOffset(Anum_hypertable_chunk_sizing_func_name)]);
	Assert(!nulls[AttrNumberGetAttrOffset(Anum_hypertable_chunk_target_size)]);
	Assert(!nulls[AttrNumberGetAttrOffset(Anum_hypertable_compression_state)]);
	Assert(!nulls[AttrNumberGetAttrOffset(Anum_hypertable_status)]);

	fd->id = DatumGetInt32(values[AttrNumberGetAttrOffset(Anum_hypertable_id)]);
	namestrcpy(&fd->schema_name,
			   DatumGetCString(values[AttrNumberGetAttrOffset(Anum_hypertable_schema_name)]));
	namestrcpy(&fd->table_name,
			   DatumGetCString(values[AttrNumberGetAttrOffset(Anum_hypertable_table_name)]));
	namestrcpy(&fd->associated_schema_name,
			   DatumGetCString(
				   values[AttrNumberGetAttrOffset(Anum_hypertable_associated_schema_name)]));
	namestrcpy(&fd->associated_table_prefix,
			   DatumGetCString(
				   values[AttrNumberGetAttrOffset(Anum_hypertable_associated_table_prefix)]));

	fd->num_dimensions =
		DatumGetInt16(values[AttrNumberGetAttrOffset(Anum_hypertable_num_dimensions)]);

	namestrcpy(&fd->chunk_sizing_func_schema,
			   DatumGetCString(
				   values[AttrNumberGetAttrOffset(Anum_hypertable_chunk_sizing_func_schema)]));
	namestrcpy(&fd->chunk_sizing_func_name,
			   DatumGetCString(
				   values[AttrNumberGetAttrOffset(Anum_hypertable_chunk_sizing_func_name)]));

	fd->chunk_target_size =
		DatumGetInt64(values[AttrNumberGetAttrOffset(Anum_hypertable_chunk_target_size)]);
	fd->compression_state =
		DatumGetInt16(values[AttrNumberGetAttrOffset(Anum_hypertable_compression_state)]);

	if (nulls[AttrNumberGetAttrOffset(Anum_hypertable_compressed_hypertable_id)])
		fd->compressed_hypertable_id = INVALID_HYPERTABLE_ID;
	else
		fd->compressed_hypertable_id = DatumGetInt32(
			values[AttrNumberGetAttrOffset(Anum_hypertable_compressed_hypertable_id)]);
	fd->status = DatumGetInt32(values[AttrNumberGetAttrOffset(Anum_hypertable_status)]);

	if (should_free)
		heap_freetuple(tuple);
}

Hypertable *
ts_hypertable_from_tupleinfo(const TupleInfo *ti)
{
	Hypertable *h = MemoryContextAllocZero(ti->mctx, sizeof(Hypertable));

	ts_hypertable_formdata_fill(&h->fd, ti);
	h->main_table_relid =
		ts_get_relation_relid(NameStr(h->fd.schema_name), NameStr(h->fd.table_name), true);
	h->space = ts_dimension_scan(h->fd.id, h->main_table_relid, h->fd.num_dimensions, ti->mctx);
	h->chunk_cache =
		ts_subspace_store_init(h->space, ti->mctx, ts_guc_max_cached_chunks_per_hypertable);
	h->chunk_sizing_func = get_chunk_sizing_func_oid(&h->fd);

	if (ts_guc_enable_chunk_skipping)
	{
		h->range_space =
			ts_chunk_column_stats_range_space_scan(h->fd.id, h->main_table_relid, ti->mctx);
	}

	return h;
}

/*
 * Find either the hypertable or the materialized hypertable, if the relid is
 * a continuous aggregate, for the relid.
 *
 * If allow_matht is false, relid should be a cagg or a hypertable.
 * If allow_matht is true, materialized hypertable is also permitted as relid
 */
Hypertable *
ts_resolve_hypertable_from_table_or_cagg(Cache *hcache, Oid relid, bool allow_matht)
{
	const char *rel_name;
	Hypertable *ht;

	rel_name = get_rel_name(relid);

	if (!rel_name)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("invalid hypertable or continuous aggregate")));

	ht = ts_hypertable_cache_get_entry(hcache, relid, CACHE_FLAG_MISSING_OK);

	if (ht)
	{
		const ContinuousAggHypertableStatus status = ts_continuous_agg_hypertable_status(ht->fd.id);
		switch (status)
		{
			case HypertableIsMaterialization:
			case HypertableIsMaterializationAndRaw:
				if (!allow_matht)
				{
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("operation not supported on materialized hypertable"),
							 errhint("Try the operation on the continuous aggregate instead."),
							 errdetail("Hypertable \"%s\" is a materialized hypertable.",
									   rel_name)));
				}
				break;

			default:
				break;
		}
	}
	else
	{
		ContinuousAgg *const cagg = ts_continuous_agg_find_by_relid(relid);

		if (!cagg)
			ereport(ERROR,
					(errcode(ERRCODE_TS_HYPERTABLE_NOT_EXIST),
					 errmsg("\"%s\" is not a hypertable or a continuous aggregate", rel_name),
					 errhint("The operation is only possible on a hypertable or continuous"
							 " aggregate.")));

		ht = ts_hypertable_get_by_id(cagg->data.mat_hypertable_id);

		if (!ht)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("no materialized table for continuous aggregate"),
					 errdetail("Continuous aggregate \"%s\" had a materialized hypertable"
							   " with id %d but it was not found in the hypertable "
							   "catalog.",
							   rel_name,
							   cagg->data.mat_hypertable_id)));
	}

	return ht;
}

static ScanTupleResult
hypertable_tuple_get_relid(TupleInfo *ti, void *data)
{
	Oid *relid = data;
	FormData_hypertable fd;
	Oid schema_oid;

	ts_hypertable_formdata_fill(&fd, ti);
	schema_oid = get_namespace_oid(NameStr(fd.schema_name), true);

	if (OidIsValid(schema_oid))
		*relid = get_relname_relid(NameStr(fd.table_name), schema_oid);

	return SCAN_DONE;
}

Oid
ts_hypertable_id_to_relid(int32 hypertable_id, bool return_invalid)
{
	Catalog *catalog = ts_catalog_get();
	Oid relid = InvalidOid;
	ScanKeyData scankey[1];
	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, HYPERTABLE),
		.index = catalog_get_index(catalog, HYPERTABLE, HYPERTABLE_ID_INDEX),
		.nkeys = 1,
		.scankey = scankey,
		.tuple_found = hypertable_tuple_get_relid,
		.data = &relid,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	/* Perform an index scan on the hypertable pkey. */
	ScanKeyInit(&scankey[0],
				Anum_hypertable_pkey_idx_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(hypertable_id));

	ts_scanner_scan(&scanctx);

	Ensure(return_invalid || OidIsValid(relid),
		   "unable to get valid parent Oid for hypertable %d",
		   hypertable_id);

	return relid;
}

int32
ts_hypertable_relid_to_id(Oid relid)
{
	Cache *hcache;
	Hypertable *ht = ts_hypertable_cache_get_cache_and_entry(relid, CACHE_FLAG_MISSING_OK, &hcache);
	int result = ht ? ht->fd.id : INVALID_HYPERTABLE_ID;

	ts_cache_release(&hcache);
	return result;
}

static int
hypertable_scan_limit_internal(ScanKeyData *scankey, int num_scankeys, int indexid,
							   tuple_found_func on_tuple_found, void *scandata, int limit,
							   LOCKMODE lock, MemoryContext mctx, tuple_filter_func filter)
{
	Catalog *catalog = ts_catalog_get();
	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, HYPERTABLE),
		.index = catalog_get_index(catalog, HYPERTABLE, indexid),
		.nkeys = num_scankeys,
		.scankey = scankey,
		.data = scandata,
		.limit = limit,
		.tuple_found = on_tuple_found,
		.lockmode = lock,
		.filter = filter,
		.scandirection = ForwardScanDirection,
		.result_mctx = mctx,
	};

	return ts_scanner_scan(&scanctx);
}

/* update the tuple at this tid. The assumption is that we already hold a
 * tuple exclusive lock and no other transaction can modify this tuple
 * The sequence of operations for any update is:
 * lock the tuple using lock_hypertable_tuple.
 * then update the required fields
 * call hypertable_update_catalog_tuple to complete the update.
 * This ensures correct tuple locking and tuple updates in the presence of
 * concurrent transactions. Failure to follow this results in catalog corruption
 */
static void
hypertable_update_catalog_tuple(ItemPointer tid, FormData_hypertable *update)
{
	HeapTuple new_tuple;
	CatalogSecurityContext sec_ctx;
	Catalog *catalog = ts_catalog_get();
	Oid table = catalog_get_table_id(catalog, HYPERTABLE);
	Relation hypertable_rel = relation_open(table, RowExclusiveLock);

	new_tuple = hypertable_formdata_make_tuple(update, hypertable_rel->rd_att);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_update_tid(hypertable_rel, tid, new_tuple);
	ts_catalog_restore_user(&sec_ctx);
	heap_freetuple(new_tuple);
	relation_close(hypertable_rel, NoLock);
}

static bool
lock_hypertable_tuple(int32 htid, ItemPointer tid, FormData_hypertable *form)
{
	bool success = false;
	ScanTupLock scantuplock = {
		.waitpolicy = LockWaitBlock,
		.lockmode = LockTupleExclusive,
	};
	ScanIterator iterator = ts_scan_iterator_create(HYPERTABLE, RowShareLock, CurrentMemoryContext);
	iterator.ctx.index = catalog_get_index(ts_catalog_get(), HYPERTABLE, HYPERTABLE_ID_INDEX);
	iterator.ctx.tuplock = &scantuplock;
	/* Keeping the lock since we presumably want to update the tuple */
	iterator.ctx.flags = SCANNER_F_KEEPLOCK;

	/* see table_tuple_lock for details about flags that are set in TupleExclusive mode */
	scantuplock.lockflags = TUPLE_LOCK_FLAG_LOCK_UPDATE_IN_PROGRESS;
	if (!IsolationUsesXactSnapshot())
	{
		/* in read committed mode, we follow all updates to this tuple */
		scantuplock.lockflags |= TUPLE_LOCK_FLAG_FIND_LAST_VERSION;
	}

	ts_scan_iterator_scan_key_init(&iterator,
								   Anum_hypertable_pkey_idx_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(htid));

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		if (ti->lockresult != TM_Ok)
		{
			if (IsolationUsesXactSnapshot())
			{
				/* For Repeatable Read and Serializable isolation level report error
				 * if we cannot lock the tuple
				 */
				ereport(ERROR,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("could not serialize access due to concurrent update")));
			}
			else
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("unable to lock hypertable catalog tuple, lock result is %d for "
								"hypertable "
								"ID (%d)",
								ti->lockresult,
								htid)));
			}
		}
		ts_hypertable_formdata_fill(form, ti);
		ItemPointer result_tid = ts_scanner_get_tuple_tid(ti);
		tid->ip_blkid = result_tid->ip_blkid;
		tid->ip_posid = result_tid->ip_posid;
		success = true;
		break;
	}
	ts_scan_iterator_close(&iterator);
	return success;
}

int
ts_hypertable_scan_with_memory_context(const char *schema, const char *table,
									   tuple_found_func tuple_found, void *data, LOCKMODE lockmode,
									   MemoryContext mctx)
{
	ScanKeyData scankey[2];
	NameData schema_name = { .data = { 0 } };
	NameData table_name = { .data = { 0 } };

	if (schema)
		namestrcpy(&schema_name, schema);

	if (table)
		namestrcpy(&table_name, table);

	/* Perform an index scan on schema and table. */
	ScanKeyInit(&scankey[0],
				Anum_hypertable_name_idx_table,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				NameGetDatum(&table_name));
	ScanKeyInit(&scankey[1],
				Anum_hypertable_name_idx_schema,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				NameGetDatum(&schema_name));

	return hypertable_scan_limit_internal(scankey,
										  2,
										  HYPERTABLE_NAME_INDEX,
										  tuple_found,
										  data,
										  1,
										  lockmode,
										  mctx,
										  NULL);
}

TSDLLEXPORT ObjectAddress
ts_hypertable_create_trigger(const Hypertable *ht, CreateTrigStmt *stmt, const char *query)
{
	ObjectAddress root_trigger_addr;
	List *chunks;
	ListCell *lc;
	int sec_ctx;
	Oid saved_uid;
	Oid owner;

	Assert(ht != NULL);

	/* create the trigger on the root table */
	/* ACL permissions checks happen within this call */
	root_trigger_addr = CreateTrigger(stmt,
									  query,
									  InvalidOid,
									  InvalidOid,
									  InvalidOid,
									  InvalidOid,
									  InvalidOid,
									  InvalidOid,
									  NULL,
									  false,
									  false);

	/* and forward it to the chunks */
	CommandCounterIncrement();

	if (!stmt->row)
		return root_trigger_addr;

	/* switch to the hypertable owner's role -- note that this logic must be the same as
	 * `ts_trigger_create_all_on_chunk` */
	owner = ts_rel_get_owner(ht->main_table_relid);
	GetUserIdAndSecContext(&saved_uid, &sec_ctx);
	if (saved_uid != owner)
		SetUserIdAndSecContext(owner, sec_ctx | SECURITY_LOCAL_USERID_CHANGE);

	chunks = find_inheritance_children(ht->main_table_relid, NoLock);

	foreach (lc, chunks)
	{
		Oid chunk_oid = lfirst_oid(lc);
		char *relschema = get_namespace_name(get_rel_namespace(chunk_oid));
		char *relname = get_rel_name(chunk_oid);
		char relkind = get_rel_relkind(chunk_oid);

		Assert(relkind == RELKIND_RELATION || relkind == RELKIND_FOREIGN_TABLE);

		/* Only create triggers on standard relations and not on, e.g., foreign
		 * table chunks */
		if (relkind == RELKIND_RELATION)
			ts_trigger_create_on_chunk(root_trigger_addr.objectId, relschema, relname);
	}

	if (saved_uid != owner)
		SetUserIdAndSecContext(saved_uid, sec_ctx);

	return root_trigger_addr;
}

TSDLLEXPORT void
ts_hypertable_drop_invalidation_replication_slot(const char *slot_name)
{
	CatalogSecurityContext sec_ctx;
	NameData slot;
	namestrcpy(&slot, slot_name);
	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	DirectFunctionCall1(pg_drop_replication_slot, NameGetDatum(&slot));
	ts_catalog_restore_user(&sec_ctx);
}

/* based on RemoveObjects */
TSDLLEXPORT void
ts_hypertable_drop_trigger(Oid relid, const char *trigger_name)
{
	List *chunks = find_inheritance_children(relid, NoLock);
	ListCell *lc;

	if (OidIsValid(relid))
	{
		ObjectAddress objaddr = {
			.classId = TriggerRelationId,
			.objectId = get_trigger_oid(relid, trigger_name, true),
		};
		if (OidIsValid(objaddr.objectId))
			performDeletion(&objaddr, DROP_RESTRICT, 0);
	}

	foreach (lc, chunks)
	{
		Oid chunk_oid = lfirst_oid(lc);
		ObjectAddress objaddr = {
			.classId = TriggerRelationId,
			.objectId = get_trigger_oid(chunk_oid, trigger_name, true),
		};

		if (OidIsValid(objaddr.objectId))
			performDeletion(&objaddr, DROP_RESTRICT, 0);
	}
}

static ScanTupleResult
hypertable_tuple_delete(TupleInfo *ti, void *data)
{
	CatalogSecurityContext sec_ctx;
	bool isnull;
	bool compressed_hypertable_id_isnull;
	int hypertable_id = DatumGetInt32(slot_getattr(ti->slot, Anum_hypertable_id, &isnull));
	int compressed_hypertable_id =
		DatumGetInt32(slot_getattr(ti->slot,
								   Anum_hypertable_compressed_hypertable_id,
								   &compressed_hypertable_id_isnull));

	ts_tablespace_delete(hypertable_id, NULL, InvalidOid);
	ts_chunk_delete_by_hypertable_id(hypertable_id);
	ts_dimension_delete_by_hypertable_id(hypertable_id, true);

	/* Also remove any policy argument / job that uses this hypertable */
	ts_bgw_policy_delete_by_hypertable_id(hypertable_id);

	/* Also remove any rows in _timescaledb_catalog.chunk_column_stats corresponding to this
	 * hypertable
	 */
	ts_chunk_column_stats_delete_by_hypertable_id(hypertable_id);

	/* Remove any dependent continuous aggs */
	ts_continuous_agg_drop_hypertable_callback(hypertable_id);

	if (!compressed_hypertable_id_isnull)
	{
		Hypertable *compressed_hypertable = ts_hypertable_get_by_id(compressed_hypertable_id);
		/* The hypertable may have already been deleted by a cascade */
		if (compressed_hypertable != NULL)
			ts_hypertable_drop(compressed_hypertable, DROP_RESTRICT);
	}

	hypertable_drop_hook_type osm_htdrop_hook = ts_get_osm_hypertable_drop_hook();
	/* Invoke the OSM callback if set */
	if (osm_htdrop_hook)
	{
		Name schema_name =
			DatumGetName(slot_getattr(ti->slot, Anum_hypertable_schema_name, &isnull));
		Name table_name = DatumGetName(slot_getattr(ti->slot, Anum_hypertable_table_name, &isnull));

		osm_htdrop_hook(NameStr(*schema_name), NameStr(*table_name));
	}

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));
	ts_catalog_restore_user(&sec_ctx);

	return SCAN_CONTINUE;
}

int
ts_hypertable_delete_by_name(const char *schema_name, const char *table_name)
{
	ScanKeyData scankey[2];

	ScanKeyInit(&scankey[0],
				Anum_hypertable_name_idx_table,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				CStringGetDatum(table_name));

	ScanKeyInit(&scankey[1],
				Anum_hypertable_name_idx_schema,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				CStringGetDatum(schema_name));
	return hypertable_scan_limit_internal(scankey,
										  2,
										  HYPERTABLE_NAME_INDEX,
										  hypertable_tuple_delete,
										  NULL,
										  0,
										  RowExclusiveLock,
										  CurrentMemoryContext,
										  NULL);
}

int
ts_hypertable_delete_by_id(int32 hypertable_id)
{
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_hypertable_pkey_idx_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(hypertable_id));

	return hypertable_scan_limit_internal(scankey,
										  1,
										  HYPERTABLE_ID_INDEX,
										  hypertable_tuple_delete,
										  NULL,
										  1,
										  RowExclusiveLock,
										  CurrentMemoryContext,
										  NULL);
}

void
ts_hypertable_drop(Hypertable *hypertable, DropBehavior behavior)
{
	/* The actual table might have been deleted already, but we still need to
	 * clean up the catalog entry. */
	if (OidIsValid(hypertable->main_table_relid))
	{
		ObjectAddress hypertable_addr = (ObjectAddress){
			.classId = RelationRelationId,
			.objectId = hypertable->main_table_relid,
		};

		/* Drop the postgres table */
		ts_compression_settings_delete(hypertable->main_table_relid);
		performDeletion(&hypertable_addr, behavior, 0);
	}

	/* Clean up catalog */
	ts_hypertable_delete_by_name(NameStr(hypertable->fd.schema_name),
								 NameStr(hypertable->fd.table_name));
}

static ScanTupleResult
reset_associated_tuple_found(TupleInfo *ti, void *data)
{
	HeapTuple new_tuple;
	FormData_hypertable fd;
	CatalogSecurityContext sec_ctx;

	ts_hypertable_formdata_fill(&fd, ti);
	namestrcpy(&fd.associated_schema_name, INTERNAL_SCHEMA_NAME);
	new_tuple = hypertable_formdata_make_tuple(&fd, ts_scanner_get_tupledesc(ti));
	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_update_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti), new_tuple);
	ts_catalog_restore_user(&sec_ctx);
	heap_freetuple(new_tuple);

	return SCAN_CONTINUE;
}

/*
 * Reset the matching associated schema to the internal schema.
 */
int
ts_hypertable_reset_associated_schema_name(const char *associated_schema)
{
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_hypertable_associated_schema_name,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				CStringGetDatum(associated_schema));

	return hypertable_scan_limit_internal(scankey,
										  1,
										  INVALID_INDEXID,
										  reset_associated_tuple_found,
										  NULL,
										  0,
										  RowExclusiveLock,
										  CurrentMemoryContext,
										  NULL);
}

int
ts_hypertable_set_name(Hypertable *ht, const char *newname)
{
	FormData_hypertable form;
	ItemPointerData tid;
	/* lock the tuple entry in the catalog table */
	bool found = lock_hypertable_tuple(ht->fd.id, &tid, &form);
	Ensure(found, "hypertable id %d not found", ht->fd.id);

	namestrcpy(&form.table_name, newname);
	hypertable_update_catalog_tuple(&tid, &form);
	return true;
}

int
ts_hypertable_set_schema(Hypertable *ht, const char *newname)
{
	FormData_hypertable form;
	ItemPointerData tid;
	/* lock the tuple entry in the catalog table */
	bool found = lock_hypertable_tuple(ht->fd.id, &tid, &form);
	Ensure(found, "hypertable id %d not found", ht->fd.id);

	namestrcpy(&form.schema_name, newname);
	hypertable_update_catalog_tuple(&tid, &form);
	return true;
}

int
ts_hypertable_set_num_dimensions(Hypertable *ht, int16 num_dimensions)
{
	FormData_hypertable form;
	ItemPointerData tid;
	/* lock the tuple entry in the catalog table */
	bool found = lock_hypertable_tuple(ht->fd.id, &tid, &form);
	Ensure(found, "hypertable id %d not found", ht->fd.id);

	Assert(num_dimensions > 0);
	form.num_dimensions = num_dimensions;
	hypertable_update_catalog_tuple(&tid, &form);
	return true;
}

#define DEFAULT_ASSOCIATED_TABLE_PREFIX_FORMAT "_hyper_%d"
static const size_t MAXIMUM_PREFIX_LENGTH = NAMEDATALEN - 16;

static void
hypertable_insert_relation(Relation rel, FormData_hypertable *fd)
{
	HeapTuple new_tuple;
	CatalogSecurityContext sec_ctx;

	new_tuple = hypertable_formdata_make_tuple(fd, RelationGetDescr(rel));

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert(rel, new_tuple);
	ts_catalog_restore_user(&sec_ctx);
	heap_freetuple(new_tuple);
}

static void
hypertable_insert(int32 hypertable_id, Name schema_name, Name table_name,
				  Name associated_schema_name, Name associated_table_prefix,
				  Name chunk_sizing_func_schema, Name chunk_sizing_func_name,
				  int64 chunk_target_size, int16 num_dimensions, bool compressed)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel;
	FormData_hypertable fd;

	fd.id = hypertable_id;
	if (fd.id == INVALID_HYPERTABLE_ID)
	{
		CatalogSecurityContext sec_ctx;
		ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
		fd.id = ts_catalog_table_next_seq_id(ts_catalog_get(), HYPERTABLE);
		ts_catalog_restore_user(&sec_ctx);
	}

	namestrcpy(&fd.schema_name, NameStr(*schema_name));
	namestrcpy(&fd.table_name, NameStr(*table_name));
	namestrcpy(&fd.associated_schema_name, NameStr(*associated_schema_name));

	if (NULL == associated_table_prefix)
	{
		NameData default_associated_table_prefix;
		memset(NameStr(default_associated_table_prefix), '\0', NAMEDATALEN);
		snprintf(NameStr(default_associated_table_prefix),
				 NAMEDATALEN,
				 DEFAULT_ASSOCIATED_TABLE_PREFIX_FORMAT,
				 fd.id);
		namestrcpy(&fd.associated_table_prefix, NameStr(default_associated_table_prefix));
	}
	else
	{
		namestrcpy(&fd.associated_table_prefix, NameStr(*associated_table_prefix));
	}
	if (strnlen(NameStr(fd.associated_table_prefix), NAMEDATALEN) > MAXIMUM_PREFIX_LENGTH)
		elog(ERROR, "associated_table_prefix too long");

	fd.num_dimensions = num_dimensions;

	namestrcpy(&fd.chunk_sizing_func_schema, NameStr(*chunk_sizing_func_schema));
	namestrcpy(&fd.chunk_sizing_func_name, NameStr(*chunk_sizing_func_name));

	fd.chunk_target_size = chunk_target_size;
	if (fd.chunk_target_size < 0)
		fd.chunk_target_size = 0;

	if (compressed)
		fd.compression_state = HypertableInternalCompressionTable;
	else
		fd.compression_state = HypertableCompressionOff;

	/* when creating a hypertable, there is never an associated compressed dual */
	fd.compressed_hypertable_id = INVALID_HYPERTABLE_ID;

	/* new hypertable does not have OSM chunk */
	fd.status = HYPERTABLE_STATUS_DEFAULT;

	rel = table_open(catalog_get_table_id(catalog, HYPERTABLE), RowExclusiveLock);
	hypertable_insert_relation(rel, &fd);
	table_close(rel, RowExclusiveLock);
}

static ScanTupleResult
hypertable_tuple_found(TupleInfo *ti, void *data)
{
	Hypertable **entry = (Hypertable **) data;

	*entry = ts_hypertable_from_tupleinfo(ti);
	return SCAN_DONE;
}

Hypertable *
ts_hypertable_get_by_name(const char *schema, const char *name)
{
	Hypertable *ht = NULL;

	hypertable_scan(schema, name, hypertable_tuple_found, (void *) &ht, AccessShareLock);

	return ht;
}

Hypertable *
ts_hypertable_get_by_id(int32 hypertable_id)
{
	ScanKeyData scankey[1];
	Hypertable *ht = NULL;

	ScanKeyInit(&scankey[0],
				Anum_hypertable_pkey_idx_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(hypertable_id));

	hypertable_scan_limit_internal(scankey,
								   1,
								   HYPERTABLE_ID_INDEX,
								   hypertable_tuple_found,
								   (void *) &ht,
								   1,
								   AccessShareLock,
								   CurrentMemoryContext,
								   NULL);
	return ht;
}

static void
hypertable_chunk_store_free(void *entry)
{
	ts_chunk_free((Chunk *) entry);
}

/*
 * Add the chunk to the cache that allows fast lookup of chunks
 * for a given hyperspace Point.
 */
Chunk *
ts_hypertable_chunk_store_add(const Hypertable *h, const Chunk *input_chunk)
{
	MemoryContext old_mcxt;

	/* Add the chunk to the subspace store */
	old_mcxt = MemoryContextSwitchTo(ts_subspace_store_mcxt(h->chunk_cache));
	Chunk *cached_chunk = ts_chunk_copy(input_chunk);
	ts_subspace_store_add(h->chunk_cache,
						  cached_chunk->cube,
						  cached_chunk,
						  hypertable_chunk_store_free);
	MemoryContextSwitchTo(old_mcxt);

	return cached_chunk;
}

/*
 * Create a chunk for the point, given that it does not exist yet.
 *
 * If the chunk already exists (i.e., another process beat us to it), then
 * lock the chunk with the specified lockmode. If the chunk is created, it
 * will always be locked with AccessExclusivelock.
 */
Chunk *
ts_hypertable_create_chunk_for_point(const Hypertable *h, const Point *point,
									 LOCKMODE chunk_lockmode)
{
	Assert(ts_subspace_store_get(h->chunk_cache, point) == NULL);

	Chunk *chunk = ts_chunk_create_for_point(h,
											 point,
											 NameStr(h->fd.associated_schema_name),
											 NameStr(h->fd.associated_table_prefix),
											 chunk_lockmode);

	/* Also add the chunk to the hypertable's chunk store */
	Chunk *cached_chunk = ts_hypertable_chunk_store_add(h, chunk);
	return cached_chunk;
}

/*
 * Find the chunk responsible for the given point.
 *
 * In case of a cache miss, a point scan will try to find a matching chunk. A
 * matching chunk will be locked in the given lockmode (unless NoLock is
 * specified) and added to the cache.
 *
 * If lockmode is higher than NoLock, all dimension slices will also be locked
 * in LockTupleKeyShare.
 *
 * If no chunk is found, NULL is returned. The returned chunk is owned by the
 * cache and may become invalid after some subsequent call to this function.
 * Leaks memory, so call in a short-lived context.
 */
Chunk *
ts_hypertable_find_chunk_for_point(const Hypertable *h, const Point *point, LOCKMODE lockmode)
{
	Chunk *chunk = ts_subspace_store_get(h->chunk_cache, point);

	if (!chunk)
	{
		chunk = ts_chunk_find_for_point(h, point, lockmode);

		if (chunk)
			chunk = ts_hypertable_chunk_store_add(h, chunk);
	}
	else if (!ts_chunk_lock_if_exists(chunk->table_id, lockmode))
	{
		return NULL;
	}

#ifdef USE_ASSERT_CHECKING
	if (chunk)
	{
		Relation chunk_rel = RelationIdGetRelation(chunk->table_id);
		Assert(CheckRelationLockedByMe(chunk_rel, lockmode, true));
		RelationClose(chunk_rel);
	}
#endif

	return chunk;
}

bool
ts_hypertable_has_tablespace(const Hypertable *ht, Oid tspc_oid)
{
	Tablespaces *tspcs = ts_tablespace_scan(ht->fd.id);

	return ts_tablespaces_contain(tspcs, tspc_oid);
}

static int
hypertable_get_chunk_round_robin_index(const Hypertable *ht, const Hypercube *hc)
{
	const Dimension *dim;
	const DimensionSlice *slice;
	int offset = 0;

	Assert(NULL != ht);
	Assert(NULL != hc);

	dim = hyperspace_get_closed_dimension(ht->space, 0);

	if (NULL == dim)
	{
		dim = hyperspace_get_open_dimension(ht->space, 0);
		/* Add some randomness between hypertables so that
		 * if there is no space partitions, but multiple hypertables
		 * the initial index is different for different hypertables.
		 * This protects against creating a lot of chunks on the same
		 * data node when many hypertables are created at roughly
		 * the same time, e.g., from a bootstrap script.
		 */
		offset = (int) ht->fd.id;
	}

	Assert(NULL != dim);

	slice = ts_hypercube_get_slice_by_dimension_id(hc, dim->fd.id);

	Assert(NULL != slice);

	return ts_dimension_get_slice_ordinal(dim, slice) + offset;
}

/*
 * Select a tablespace to use for a given chunk.
 *
 * Selection happens based on the first closed (space) dimension, if available,
 * otherwise the first closed (time) one.
 *
 * We try to do "sticky" selection to consistently pick the same tablespace for
 * chunks in the same closed (space) dimension. This ensures chunks in the same
 * "space" partition will live on the same disk.
 */
Tablespace *
ts_hypertable_select_tablespace(const Hypertable *ht, const Chunk *chunk)
{
	Tablespaces *tspcs = ts_tablespace_scan(ht->fd.id);
	int i;

	if (NULL == tspcs || tspcs->num_tablespaces == 0)
		return NULL;

	i = hypertable_get_chunk_round_robin_index(ht, chunk->cube);

	/* Use the index of the slice to find the tablespace */
	return &tspcs->tablespaces[i % tspcs->num_tablespaces];
}

const char *
ts_hypertable_select_tablespace_name(const Hypertable *ht, const Chunk *chunk)
{
	Tablespace *tspc = ts_hypertable_select_tablespace(ht, chunk);
	Oid main_tspc_oid;

	if (tspc != NULL)
		return NameStr(tspc->fd.tablespace_name);

	/* Use main table tablespace, if any */
	main_tspc_oid = get_rel_tablespace(ht->main_table_relid);
	if (OidIsValid(main_tspc_oid))
		return get_tablespace_name(main_tspc_oid);

	return NULL;
}

/*
 * Get the tablespace at an offset from the given tablespace.
 */
Tablespace *
ts_hypertable_get_tablespace_at_offset_from(int32 hypertable_id, Oid tablespace_oid, int16 offset)
{
	Tablespaces *tspcs = ts_tablespace_scan(hypertable_id);
	int i = 0;

	if (NULL == tspcs || tspcs->num_tablespaces == 0)
		return NULL;

	for (i = 0; i < tspcs->num_tablespaces; i++)
	{
		if (tablespace_oid == tspcs->tablespaces[i].tablespace_oid)
			return &tspcs->tablespaces[(i + offset) % tspcs->num_tablespaces];
	}

	return NULL;
}

static inline Oid
hypertable_relid_lookup(Oid relid)
{
	Cache *hcache;
	Hypertable *ht = ts_hypertable_cache_get_cache_and_entry(relid, CACHE_FLAG_MISSING_OK, &hcache);
	Oid result = (ht == NULL) ? InvalidOid : ht->main_table_relid;

	ts_cache_release(&hcache);

	return result;
}

/*
 * Returns a hypertable's relation ID (OID) iff the given RangeVar corresponds to
 * a hypertable, otherwise InvalidOid.
 */
Oid
ts_hypertable_relid(RangeVar *rv)
{
	return hypertable_relid_lookup(RangeVarGetRelid(rv, NoLock, true));
}

bool
ts_is_hypertable(Oid relid)
{
	if (!OidIsValid(relid))
		return false;

	return OidIsValid(hypertable_relid_lookup(relid));
}

/*
 * Check that the current user can create chunks in a hypertable's associated
 * schema.
 *
 * This function is typically called from create_hypertable() to verify that the
 * table owner has CREATE permissions for the schema (if it already exists) or
 * the database (if the schema does not exist and needs to be created).
 */
static Oid
hypertable_check_associated_schema_permissions(const char *schema_name, Oid user_oid)
{
	Oid schema_oid;

	/*
	 * If the schema name is NULL, it implies the internal catalog schema and
	 * anyone should be able to create chunks there.
	 */
	if (NULL == schema_name)
		return InvalidOid;

	schema_oid = get_namespace_oid(schema_name, true);

	/* Anyone can create chunks in the internal schema */
	if (strncmp(schema_name, INTERNAL_SCHEMA_NAME, NAMEDATALEN) == 0)
	{
		Assert(OidIsValid(schema_oid));
		return schema_oid;
	}

	if (!OidIsValid(schema_oid))
	{
		/*
		 * Schema does not exist, so we must check that the user has
		 * privileges to create the schema in the current database
		 */
		if (object_aclcheck(DatabaseRelationId, MyDatabaseId, user_oid, ACL_CREATE) != ACLCHECK_OK)
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("permissions denied: cannot create schema \"%s\" in database \"%s\"",
							schema_name,
							get_database_name(MyDatabaseId))));
	}
	else if (object_aclcheck(NamespaceRelationId, schema_oid, user_oid, ACL_CREATE) != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permissions denied: cannot create chunks in schema \"%s\"", schema_name)));

	return schema_oid;
}

inline static bool
table_has_rules(Relation rel)
{
	return rel->rd_rules != NULL;
}

bool
ts_hypertable_has_chunks(Oid table_relid, LOCKMODE lockmode)
{
	return find_inheritance_children(table_relid, lockmode) != NIL;
}

static void
hypertable_create_schema(const char *schema_name)
{
	CreateSchemaStmt stmt = {
		.schemaname = (char *) schema_name,
		.authrole = NULL,
		.schemaElts = NIL,
		.if_not_exists = true,
	};

	CreateSchemaCommand(&stmt, "(generated CREATE SCHEMA command)", -1, -1);
}

/*
 * Check that existing table constraints are supported.
 *
 * Hypertables do not support the following constraints:
 *
 * - NO INHERIT constraints cannot be enforced on a hypertable since they only
 *   exist on the parent table, which will have no tuples.
 * - FOREIGN KEY constraints referencing a hypertable.
 */
static void
hypertable_validate_constraints(Oid relid)
{
	Relation catalog;
	SysScanDesc scan;
	ScanKeyData scankey;
	HeapTuple tuple;

	catalog = table_open(ConstraintRelationId, AccessShareLock);

	ScanKeyInit(&scankey,
				Anum_pg_constraint_conrelid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(relid));

	scan = systable_beginscan(catalog, ConstraintRelidTypidNameIndexId, true, NULL, 1, &scankey);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_constraint form = (Form_pg_constraint) GETSTRUCT(tuple);

		if (form->contype == CONSTRAINT_FOREIGN)
		{
			if (ts_hypertable_relid_to_id(form->confrelid) != INVALID_HYPERTABLE_ID &&
				!is_partitioning_allowed(form->confrelid))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("hypertables cannot be used as foreign key references of "
								"hypertables")));
		}

		if (form->contype == CONSTRAINT_CHECK && form->connoinherit)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("cannot have NO INHERIT constraints on hypertable \"%s\"",
							get_rel_name(relid)),
					 errhint("Remove all NO INHERIT constraints from table \"%s\" before "
							 "making it a hypertable.",
							 get_rel_name(relid))));
	}

	systable_endscan(scan);

	/* Check for foreign keys that reference this table */
	ScanKeyInit(&scankey,
				Anum_pg_constraint_confrelid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(relid));

	scan = systable_beginscan(catalog, 0, false, NULL, 1, &scankey);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_constraint form = (Form_pg_constraint) GETSTRUCT(tuple);

		/*
		 * Hypertable <-> hypertable foreign keys are not supported.
		 */
		if (form->contype == CONSTRAINT_FOREIGN &&
			ts_hypertable_relid_to_id(form->conrelid) != INVALID_HYPERTABLE_ID)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("cannot have FOREIGN KEY constraints to hypertable \"%s\"",
							get_rel_name(relid)),
					 errhint("Remove all FOREIGN KEY constraints to table \"%s\" before "
							 "making it a hypertable.",
							 get_rel_name(relid))));
	}

	systable_endscan(scan);
	table_close(catalog, AccessShareLock);
}

static Datum
create_hypertable_datum(FunctionCallInfo fcinfo, const Hypertable *ht, bool created,
						bool is_generic)
{
	TupleDesc tupdesc;
	HeapTuple tuple;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in "
						"context that cannot accept type record")));

	tupdesc = BlessTupleDesc(tupdesc);

	if (is_generic)
	{
		Datum values[Natts_generic_create_hypertable];
		bool nulls[Natts_generic_create_hypertable] = { false };

		values[AttrNumberGetAttrOffset(Anum_generic_create_hypertable_id)] =
			Int32GetDatum(ht->fd.id);
		values[AttrNumberGetAttrOffset(Anum_generic_create_hypertable_created)] =
			BoolGetDatum(created);
		tuple = heap_form_tuple(tupdesc, values, nulls);
	}
	else
	{
		Datum values[Natts_create_hypertable];
		bool nulls[Natts_create_hypertable] = { false };

		values[AttrNumberGetAttrOffset(Anum_create_hypertable_id)] = Int32GetDatum(ht->fd.id);
		values[AttrNumberGetAttrOffset(Anum_create_hypertable_schema_name)] =
			NameGetDatum(&ht->fd.schema_name);
		values[AttrNumberGetAttrOffset(Anum_create_hypertable_table_name)] =
			NameGetDatum(&ht->fd.table_name);
		values[AttrNumberGetAttrOffset(Anum_create_hypertable_created)] = BoolGetDatum(created);
		tuple = heap_form_tuple(tupdesc, values, nulls);
	}

	return HeapTupleGetDatum(tuple);
}

TS_FUNCTION_INFO_V1(ts_hypertable_create);
TS_FUNCTION_INFO_V1(ts_hypertable_create_general);

/*
 * Create a hypertable from an existing table. The specific version of create hypertable API
 * process the function arguments before calling this function.
 */
static Datum
ts_hypertable_create_internal(FunctionCallInfo fcinfo, Oid table_relid,
							  DimensionInfo *open_dim_info, DimensionInfo *closed_dim_info,
							  Name associated_schema_name, Name associated_table_prefix,
							  bool create_default_indexes, bool if_not_exists, bool migrate_data,
							  text *target_size, Oid sizing_func, bool is_generic)
{
	Cache *hcache;
	Hypertable *ht;
	Datum retval;
	bool created;
	uint32 flags = 0;

	ts_feature_flag_check(FEATURE_HYPERTABLE);

	ChunkSizingInfo chunk_sizing_info = {
		.table_relid = table_relid,
		.target_size = target_size,
		.func = sizing_func,
		.colname = NameStr(open_dim_info->colname),
		.check_for_index = !create_default_indexes,
	};

	TS_PREVENT_FUNC_IF_READ_ONLY();

	ht = ts_hypertable_cache_get_cache_and_entry(table_relid, CACHE_FLAG_MISSING_OK, &hcache);
	if (ht)
	{
		if (if_not_exists)
			ereport(NOTICE,
					(errcode(ERRCODE_TS_HYPERTABLE_EXISTS),
					 errmsg("table \"%s\" is already a hypertable, skipping",
							get_rel_name(table_relid))));
		else
			ereport(ERROR,
					(errcode(ERRCODE_TS_HYPERTABLE_EXISTS),
					 errmsg("table \"%s\" is already a hypertable", get_rel_name(table_relid))));
		created = false;
	}
	else
	{
		/* Release previously pinned cache */
		ts_cache_release(&hcache);

		if (closed_dim_info && !closed_dim_info->num_slices_is_set)
		{
			/* If the number of partitions isn't specified, default to setting it
			 * to the number of data nodes */
			int16 num_partitions = closed_dim_info->num_slices;
			closed_dim_info->num_slices = num_partitions;
			closed_dim_info->num_slices_is_set = true;
		}

		if (if_not_exists)
			flags |= HYPERTABLE_CREATE_IF_NOT_EXISTS;
		if (!create_default_indexes)
			flags |= HYPERTABLE_CREATE_DISABLE_DEFAULT_INDEXES;
		if (migrate_data)
			flags |= HYPERTABLE_CREATE_MIGRATE_DATA;

		created = ts_hypertable_create_from_info(table_relid,
												 INVALID_HYPERTABLE_ID,
												 flags,
												 open_dim_info,
												 closed_dim_info,
												 associated_schema_name,
												 associated_table_prefix,
												 &chunk_sizing_info);

		Assert(created);
		ht = ts_hypertable_cache_get_cache_and_entry(table_relid, CACHE_FLAG_NONE, &hcache);
	}

	retval = create_hypertable_datum(fcinfo, ht, created, is_generic);
	ts_cache_release(&hcache);

	PG_RETURN_DATUM(retval);
}

/*
 * Process create_hypertable parameters for time specific implementation.
 *
 * Arguments:
 * relation                REGCLASS
 * time_column_name        NAME
 * partitioning_column     NAME = NULL
 * number_partitions       INTEGER = NULL
 * associated_schema_name  NAME = NULL
 * associated_table_prefix NAME = NULL
 * chunk_time_interval     anyelement = NULL::BIGINT
 * create_default_indexes  BOOLEAN = TRUE
 * if_not_exists           BOOLEAN = FALSE
 * partitioning_func       REGPROC = NULL
 * migrate_data            BOOLEAN = FALSE
 * chunk_target_size       TEXT = NULL
 * chunk_sizing_func       OID = NULL
 * time_partitioning_func  REGPROC = NULL
 */
Datum
ts_hypertable_create(PG_FUNCTION_ARGS)
{
	Oid table_relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	Name open_dim_name = PG_ARGISNULL(1) ? NULL : PG_GETARG_NAME(1);
	Name closed_dim_name = PG_ARGISNULL(2) ? NULL : PG_GETARG_NAME(2);
	int16 num_partitions = PG_ARGISNULL(3) ? -1 : PG_GETARG_INT16(3);
	Name associated_schema_name = PG_ARGISNULL(4) ? NULL : PG_GETARG_NAME(4);
	Name associated_table_prefix = PG_ARGISNULL(5) ? NULL : PG_GETARG_NAME(5);
	Datum default_interval = PG_ARGISNULL(6) ? UnassignedDatum : PG_GETARG_DATUM(6);
	Oid interval_type = PG_ARGISNULL(6) ? InvalidOid : get_fn_expr_argtype(fcinfo->flinfo, 6);
	bool create_default_indexes =
		PG_ARGISNULL(7) ? false : PG_GETARG_BOOL(7); /* Defaults to true in the sql code */
	bool if_not_exists = PG_ARGISNULL(8) ? false : PG_GETARG_BOOL(8);
	regproc closed_partitioning_func = PG_ARGISNULL(9) ? InvalidOid : PG_GETARG_OID(9);
	bool migrate_data = PG_ARGISNULL(10) ? false : PG_GETARG_BOOL(10);
	text *target_size = PG_ARGISNULL(11) ? NULL : PG_GETARG_TEXT_P(11);
	Oid sizing_func = PG_ARGISNULL(12) ? InvalidOid : PG_GETARG_OID(12);
	regproc open_partitioning_func = PG_ARGISNULL(13) ? InvalidOid : PG_GETARG_OID(13);
	const int origin_paramnum = 14;
	bool origin_isnull = PG_ARGISNULL(origin_paramnum);
	Datum origin = origin_isnull ? 0 : PG_GETARG_DATUM(origin_paramnum);
	Oid origin_type =
		origin_isnull ? InvalidOid : get_fn_expr_argtype(fcinfo->flinfo, origin_paramnum);

	if (!OidIsValid(table_relid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("relation cannot be NULL")));

	if (!open_dim_name)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("partition column cannot be NULL")));

	DimensionInfo *open_dim_info =
		ts_dimension_info_create_open(table_relid,
									  open_dim_name,		  /* column name */
									  default_interval,		  /* interval */
									  interval_type,		  /* interval type */
									  open_partitioning_func, /* partitioning func */
									  origin,				  /* origin */
									  origin_type,			  /* origin_type */
									  !origin_isnull /* has_origin */);

	DimensionInfo *closed_dim_info = NULL;
	if (closed_dim_name)
		closed_dim_info =
			ts_dimension_info_create_closed(table_relid,
											closed_dim_name,		 /* column name */
											num_partitions,			 /* number partitions */
											closed_partitioning_func /* partitioning func */
			);

	return ts_hypertable_create_internal(fcinfo,
										 table_relid,
										 open_dim_info,
										 closed_dim_info,
										 associated_schema_name,
										 associated_table_prefix,
										 create_default_indexes,
										 if_not_exists,
										 migrate_data,
										 target_size,
										 sizing_func,
										 false);
}

static Oid
get_sizing_func_oid()
{
	const char *sizing_func_name = "calculate_chunk_interval";
	const int sizing_func_nargs = 3;
	static Oid sizing_func_arg_types[] = { INT4OID, INT8OID, INT8OID };

	return ts_get_function_oid(sizing_func_name,
							   FUNCTIONS_SCHEMA_NAME,
							   sizing_func_nargs,
							   sizing_func_arg_types);
}

/*
 * Process create_hypertable parameters for generic implementation.
 *
 * Arguments:
 * relation                REGCLASS
 * dimension               dimension_info
 * create_default_indexes  BOOLEAN = TRUE
 * if_not_exists           BOOLEAN = FALSE
 * migrate_data            BOOLEAN = FALSE
 */
Datum
ts_hypertable_create_general(PG_FUNCTION_ARGS)
{
	Oid table_relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	DimensionInfo *dim_info = NULL;
	GETARG_NOTNULL_POINTER(dim_info, 1, "dimension", DimensionInfo);
	bool create_default_indexes = PG_ARGISNULL(2) ? false : PG_GETARG_BOOL(2);
	bool if_not_exists = PG_ARGISNULL(3) ? false : PG_GETARG_BOOL(3);
	bool migrate_data = PG_ARGISNULL(4) ? false : PG_GETARG_BOOL(4);

	/*
	 * We do not support closed (hash) dimensions for the main partitioning
	 * column. Check that first. The behavior then becomes consistent with the
	 * earlier "ts_hypertable_create" implementation.
	 */
	if (IS_CLOSED_DIMENSION(dim_info))
	{
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot partition using a closed dimension on primary column"),
				 errhint("Use range partitioning on the primary column.")));
	}

	/*
	 * Current implementation requires to provide a valid chunk sizing function
	 * that is being used to populate hypertable catalog information.
	 */
	Oid sizing_func = get_sizing_func_oid();

	/*
	 * Fill in the rest of the info.
	 */
	AttrNumber colattr = get_attnum(table_relid, NameStr(dim_info->colname));
	dim_info->table_relid = table_relid;
	dim_info->coltype = get_atttype(table_relid, colattr);
	ts_dimension_info_set_defaults(dim_info);

	return ts_hypertable_create_internal(fcinfo,
										 table_relid,
										 dim_info,
										 NULL, /* closed_dim_info */
										 NULL, /* associated_schema_name */
										 NULL, /* associated_table_prefix */
										 create_default_indexes,
										 if_not_exists,
										 migrate_data,
										 NULL,
										 sizing_func,
										 true);
}

/* Go through columns of parent table and check for column data types. */
static void
ts_validate_basetable_columns(Relation *rel)
{
	int attno;
	TupleDesc tupdesc;

	tupdesc = RelationGetDescr(*rel);
	for (attno = 1; attno <= tupdesc->natts; attno++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, attno - 1);
		/* skip dropped columns */
		if (attr->attisdropped)
			continue;
		Oid typid = attr->atttypid;
		switch (typid)
		{
			case CHAROID:
			case VARCHAROID:
				ereport(WARNING,
						(errmsg("column type \"%s\" used for \"%s\" does not follow best practices",
								format_type_be(typid),
								NameStr(attr->attname)),
						 errhint("Use datatype TEXT instead.")));
				break;
			case TIMESTAMPOID:
				ereport(WARNING,
						(errmsg("column type \"%s\" used for \"%s\" does not follow best practices",
								format_type_be(typid),
								NameStr(attr->attname)),
						 errhint("Use datatype TIMESTAMPTZ instead.")));
				break;
			default:
				break;
		}
	}
}

/* Creates a new hypertable.
 *
 * Flags are one of HypertableCreateFlags.
 * All parameters after tim_dim_info can be NUL
 * returns 'true' if new hypertable was created, false if 'if_not_exists' and the hypertable already
 * exists.
 */
bool
ts_hypertable_create_from_info(Oid table_relid, int32 hypertable_id, uint32 flags,
							   DimensionInfo *time_dim_info, DimensionInfo *closed_dim_info,
							   Name associated_schema_name, Name associated_table_prefix,
							   ChunkSizingInfo *chunk_sizing_info)
{
	Cache *hcache;
	Hypertable *ht;
	Oid associated_schema_oid;
	Oid user_oid = GetUserId();
	Oid tspc_oid = get_rel_tablespace(table_relid);
	bool table_has_data;
	NameData schema_name, table_name, default_associated_schema_name;
	Relation rel;
	bool if_not_exists = (flags & HYPERTABLE_CREATE_IF_NOT_EXISTS) != 0;

	/* quick exit in the easy if-not-exists case to avoid all locking */
	if (if_not_exists && ts_is_hypertable(table_relid))
	{
		ereport(NOTICE,
				(errcode(ERRCODE_TS_HYPERTABLE_EXISTS),
				 errmsg("table \"%s\" is already a hypertable, skipping",
						get_rel_name(table_relid))));

		return false;
	}

	/*
	 * Serialize hypertable creation to avoid having multiple transactions
	 * creating the same hypertable simultaneously. The lock should conflict
	 * with itself and RowExclusive, to prevent simultaneous inserts on the
	 * table. Also since TRUNCATE (part of data migrations) takes an
	 * AccessExclusiveLock take that lock level here too so that we don't have
	 * lock upgrades, which are susceptible to deadlocks. If we aren't
	 * migrating data, then shouldn't have much contention on the table thus
	 * not worth optimizing.
	 */
	rel = table_open(table_relid, AccessExclusiveLock);

	/* recheck after getting lock */
	if (ts_is_hypertable(table_relid))
	{
		/*
		 * Unlock and return. Note that unlocking is analogous to what PG does
		 * for ALTER TABLE ADD COLUMN IF NOT EXIST
		 */
		table_close(rel, AccessExclusiveLock);

		if (if_not_exists)
		{
			ereport(NOTICE,
					(errcode(ERRCODE_TS_HYPERTABLE_EXISTS),
					 errmsg("table \"%s\" is already a hypertable, skipping",
							get_rel_name(table_relid))));
			return false;
		}

		ereport(ERROR,
				(errcode(ERRCODE_TS_HYPERTABLE_EXISTS),
				 errmsg("table \"%s\" is already a hypertable", get_rel_name(table_relid))));
	}

	/*
	 * Hypertables also get created as part of caggs. Report warnings
	 * only for hypertables created via call to create_hypertable().
	 */
	if (hypertable_id == INVALID_HYPERTABLE_ID)
		ts_validate_basetable_columns(&rel);

	/*
	 * Check that the user has permissions to make this table into a
	 * hypertable
	 */
	ts_hypertable_permissions_check(table_relid, user_oid);

	/* Is this the right kind of relation? */
	switch (get_rel_relkind(table_relid))
	{
		case RELKIND_PARTITIONED_TABLE:
			if (!ts_guc_enable_partitioned_hypertables)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("table \"%s\" is already partitioned", get_rel_name(table_relid)),
						 errdetail(
							 "It is not possible to turn partitioned tables into hypertables.")));
			break;
		case RELKIND_MATVIEW:
		case RELKIND_RELATION:
			break;

		default:
			ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("invalid relation type")));
	}

	/*
	 * Check that the table is not part of any publication
	 */
	if (GetRelationPublications(table_relid) != NIL || GetAllTablesPublications() != NIL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_TS_OPERATION_NOT_SUPPORTED),
				 errmsg("cannot create hypertable for table \"%s\" because it is part of a "
						"publication",
						get_rel_name(table_relid))));
	}

	/* Check that the table doesn't have any unsupported constraints */
	hypertable_validate_constraints(table_relid);

	/* No need to check for data in partitioned tables */
	table_has_data = get_rel_relkind(table_relid) == RELKIND_PARTITIONED_TABLE ?
						 false :
						 ts_relation_has_tuples(rel);

	if ((flags & HYPERTABLE_CREATE_MIGRATE_DATA) == 0 && table_has_data)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("table \"%s\" is not empty", get_rel_name(table_relid)),
				 errhint("You can migrate data by specifying 'migrate_data => true' when calling "
						 "this function.")));

	if (is_inheritance_table(table_relid))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("table \"%s\" is already partitioned", get_rel_name(table_relid)),
				 errdetail(
					 "It is not possible to turn tables that use inheritance into hypertables.")));

	if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("table \"%s\" cannot be temporary", get_rel_name(table_relid)),
				 errdetail("It is not supported to turn temporary tables into hypertables.")));

	if (table_has_rules(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("hypertables do not support rules"),
				 errdetail("Table \"%s\" has attached rules, which do not work on hypertables.",
						   get_rel_name(table_relid)),
				 errhint("Remove the rules before creating a hypertable.")));

	/*
	 * Must close the relation to decrease the reference count for the relation
	 * as PG18+ will check the reference count when adding constraints for the table.
	 */
	table_close(rel, NoLock);
	/*
	 * Create the associated schema where chunks are stored, or, check
	 * permissions if it already exists
	 */
	if (NULL == associated_schema_name)
	{
		namestrcpy(&default_associated_schema_name, INTERNAL_SCHEMA_NAME);
		associated_schema_name = &default_associated_schema_name;
	}

	associated_schema_oid =
		hypertable_check_associated_schema_permissions(NameStr(*associated_schema_name), user_oid);

	/* Create the associated schema if it doesn't already exist */
	if (!OidIsValid(associated_schema_oid))
		hypertable_create_schema(NameStr(*associated_schema_name));

	/*
	 * Hypertables do not support arbitrary triggers, so if the table already
	 * has unsupported triggers we bail out
	 */
	ts_check_unsupported_triggers(table_relid);

	if (NULL == chunk_sizing_info)
		chunk_sizing_info = ts_chunk_sizing_info_get_default_disabled(table_relid);

	/* Validate and set chunk sizing information */
	if (OidIsValid(chunk_sizing_info->func))
	{
		ts_chunk_adaptive_sizing_info_validate(chunk_sizing_info);

		if (chunk_sizing_info->target_size_bytes > 0)
		{
			ereport(NOTICE,
					(errcode(ERRCODE_WARNING),
					 errmsg("adaptive chunking is a BETA feature and is not recommended for "
							"production deployments")));
			time_dim_info->adaptive_chunking = true;
		}
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("chunk sizing function cannot be NULL")));
	}

	/* Validate that the dimensions are OK */
	ts_dimension_info_validate(time_dim_info);

	if (DIMENSION_INFO_IS_SET(closed_dim_info))
		ts_dimension_info_validate(closed_dim_info);

	/* Checks pass, now we can create the catalog information */
	namestrcpy(&schema_name, get_namespace_name(get_rel_namespace(table_relid)));
	namestrcpy(&table_name, get_rel_name(table_relid));

	hypertable_insert(hypertable_id,
					  &schema_name,
					  &table_name,
					  associated_schema_name,
					  associated_table_prefix,
					  &chunk_sizing_info->func_schema,
					  &chunk_sizing_info->func_name,
					  chunk_sizing_info->target_size_bytes,
					  DIMENSION_INFO_IS_SET(closed_dim_info) ? 2 : 1,
					  false);

	/* Get the a Hypertable object via the cache */
	time_dim_info->ht =
		ts_hypertable_cache_get_cache_and_entry(table_relid, CACHE_FLAG_NONE, &hcache);

	/* Add validated dimensions */
	ts_dimension_add_from_info(time_dim_info);

	if (DIMENSION_INFO_IS_SET(closed_dim_info))
	{
		closed_dim_info->ht = time_dim_info->ht;
		ts_dimension_add_from_info(closed_dim_info);
	}

	/* Refresh the cache to get the updated hypertable with added dimensions */
	ts_cache_release(&hcache);
	ht = ts_hypertable_cache_get_cache_and_entry(table_relid, CACHE_FLAG_NONE, &hcache);

	/* Verify that existing indexes are compatible with a hypertable */
	ts_indexing_verify_indexes(ht);

	/* Attach tablespace, if any */
	if (OidIsValid(tspc_oid))
	{
		NameData tspc_name;

		namestrcpy(&tspc_name, get_tablespace_name(tspc_oid));
		ts_tablespace_attach_internal(&tspc_name, table_relid, false);
	}

	if ((flags & HYPERTABLE_CREATE_DISABLE_DEFAULT_INDEXES) == 0)
		ts_indexing_create_default_indexes(ht);

	/*
	 * Migrate data from the main table to chunks
	 */
	if (table_has_data)
	{
		ereport(NOTICE,
				(errmsg("migrating data to chunks"),
				 errdetail("Migration might take a while depending on the amount of data.")));

		timescaledb_move_from_table_to_chunks(ht, RowExclusiveLock);
	}

	ts_cache_release(&hcache);

	return true;
}

/* Used as a tuple found function */
static ScanTupleResult
hypertable_rename_schema_name(TupleInfo *ti, void *data)
{
	const char **schema_names = (const char **) data;
	const char *old_schema_name = schema_names[0];
	const char *new_schema_name = schema_names[1];
	bool updated = false;
	FormData_hypertable fd;

	ts_hypertable_formdata_fill(&fd, ti);

	/*
	 * Because we are doing a heap scan with no scankey, we don't know which
	 * schema name to change, if any
	 */
	if (namestrcmp(&fd.schema_name, old_schema_name) == 0)
	{
		namestrcpy(&fd.schema_name, new_schema_name);
		updated = true;
	}
	if (namestrcmp(&fd.associated_schema_name, old_schema_name) == 0)
	{
		namestrcpy(&fd.associated_schema_name, new_schema_name);
		updated = true;
	}
	if (namestrcmp(&fd.chunk_sizing_func_schema, old_schema_name) == 0)
	{
		namestrcpy(&fd.chunk_sizing_func_schema, new_schema_name);
		updated = true;
	}

	/* Only update the catalog if we explicitly something */
	if (updated)
	{
		HeapTuple new_tuple = hypertable_formdata_make_tuple(&fd, ts_scanner_get_tupledesc(ti));
		ts_catalog_update_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti), new_tuple);
		heap_freetuple(new_tuple);
	}

	/* Keep going so we can change the name for all hypertables */
	return SCAN_CONTINUE;
}

/* Go through internal hypertable table and rename all matching schemas */
void
ts_hypertables_rename_schema_name(const char *old_name, const char *new_name)
{
	const char *schema_names[2] = { old_name, new_name };
	Catalog *catalog = ts_catalog_get();

	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, HYPERTABLE),
		.index = InvalidOid,
		.tuple_found = hypertable_rename_schema_name,
		.data = (void *) schema_names,
		.lockmode = RowExclusiveLock,
		.scandirection = ForwardScanDirection,
	};

	ts_scanner_scan(&scanctx);
}

bool
ts_is_partitioning_column(const Hypertable *ht, AttrNumber column_attno)
{
	uint16 i;

	for (i = 0; i < ht->space->num_dimensions; i++)
	{
		if (column_attno == ht->space->dimensions[i].column_attno)
			return true;
	}
	return false;
}

bool
ts_is_partitioning_column_name(const Hypertable *ht, NameData column_name)
{
	uint16 i;

	for (i = 0; i < ht->space->num_dimensions; i++)
	{
		if (namestrcmp(&ht->space->dimensions[i].fd.column_name, NameStr(column_name)) == 0)
			return true;
	}
	return false;
}

static void
integer_now_func_validate(Oid now_func_oid, Oid open_dim_type)
{
	HeapTuple tuple;
	Form_pg_proc now_func;

	/* this function should only be called for hypertables with an open integer time dimension */
	Assert(IS_INTEGER_TYPE(open_dim_type));

	if (!OidIsValid(now_func_oid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION), (errmsg("invalid custom time function"))));

	tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(now_func_oid));
	if (!HeapTupleIsValid(tuple))
	{
		ereport(ERROR,
				(errcode(ERRCODE_NO_DATA_FOUND),
				 errmsg("cache lookup failed for function %u", now_func_oid)));
	}

	now_func = (Form_pg_proc) GETSTRUCT(tuple);

	if ((now_func->provolatile != PROVOLATILE_IMMUTABLE &&
		 now_func->provolatile != PROVOLATILE_STABLE) ||
		now_func->pronargs != 0)
	{
		ReleaseSysCache(tuple);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid custom time function"),
				 errhint("A custom time function must take no arguments and be STABLE.")));
	}

	if (now_func->prorettype != open_dim_type)
	{
		ReleaseSysCache(tuple);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid custom time function"),
				 errhint("The return type of the custom time function must be the same as"
						 " the type of the time column of the hypertable.")));
	}
	ReleaseSysCache(tuple);
}

TS_FUNCTION_INFO_V1(ts_hypertable_set_integer_now_func);

Datum
ts_hypertable_set_integer_now_func(PG_FUNCTION_ARGS)
{
	Oid table_relid = PG_GETARG_OID(0);
	Oid now_func_oid = PG_GETARG_OID(1);
	bool replace_if_exists = PG_GETARG_BOOL(2);
	Hypertable *hypertable;
	Cache *hcache;
	const Dimension *open_dim;
	Oid open_dim_type;
	AclResult aclresult;

	ts_hypertable_permissions_check(table_relid, GetUserId());
	hypertable = ts_hypertable_cache_get_cache_and_entry(table_relid, CACHE_FLAG_NONE, &hcache);

	if (TS_HYPERTABLE_IS_INTERNAL_COMPRESSION_TABLE(hypertable))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("custom time function not supported on internal columnstore table")));

	/* validate that the open dimension uses numeric type */
	open_dim = hyperspace_get_open_dimension(hypertable->space, 0);

	if (!replace_if_exists)
		if (*NameStr(open_dim->fd.integer_now_func_schema) != '\0' ||
			*NameStr(open_dim->fd.integer_now_func) != '\0')
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("custom time function already set for hypertable \"%s\"",
							get_rel_name(table_relid))));

	open_dim_type = ts_dimension_get_partition_type(open_dim);
	if (!IS_INTEGER_TYPE(open_dim_type))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("custom time function not supported"),
				 errhint("A custom time function can only be set for hypertables"
						 " that have integer time dimensions.")));

	integer_now_func_validate(now_func_oid, open_dim_type);

	aclresult = object_aclcheck(ProcedureRelationId, now_func_oid, GetUserId(), ACL_EXECUTE);
	if (aclresult != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for function %s", get_func_name(now_func_oid))));

	ts_dimension_update(hypertable,
						&open_dim->fd.column_name,
						DIMENSION_TYPE_OPEN,
						NULL,
						NULL,
						&now_func_oid);
	ts_cache_release(&hcache);
	PG_RETURN_NULL();
}

/*Assume permissions are already checked
 * set compression state as enabled
 */
bool
ts_hypertable_set_compressed(Hypertable *ht, int32 compressed_hypertable_id)
{
	FormData_hypertable form;
	ItemPointerData tid;
	/* lock the tuple entry in the catalog table */
	bool found = lock_hypertable_tuple(ht->fd.id, &tid, &form);
	Ensure(found, "hypertable id %d not found", ht->fd.id);

	Assert(!TS_HYPERTABLE_IS_INTERNAL_COMPRESSION_TABLE(ht));
	form.compression_state = HypertableCompressionEnabled;
	form.compressed_hypertable_id = compressed_hypertable_id;
	hypertable_update_catalog_tuple(&tid, &form);
	return true;
}

/* set compression_state as disabled and remove any
 * associated compressed hypertable id
 */
bool
ts_hypertable_unset_compressed(Hypertable *ht)
{
	FormData_hypertable form;
	ItemPointerData tid;
	/* lock the tuple entry in the catalog table */
	bool found = lock_hypertable_tuple(ht->fd.id, &tid, &form);
	Ensure(found, "hypertable id %d not found", ht->fd.id);

	Assert(!TS_HYPERTABLE_IS_INTERNAL_COMPRESSION_TABLE(ht));
	form.compression_state = HypertableCompressionOff;
	form.compressed_hypertable_id = INVALID_HYPERTABLE_ID;
	hypertable_update_catalog_tuple(&tid, &form);
	return true;
}

bool
ts_hypertable_set_compress_interval(Hypertable *ht, int64 compress_interval)
{
	Assert(!TS_HYPERTABLE_IS_INTERNAL_COMPRESSION_TABLE(ht));

	Dimension *time_dimension =
		ts_hyperspace_get_mutable_dimension(ht->space, DIMENSION_TYPE_OPEN, 0);

	return ts_dimension_set_compress_interval(time_dimension, compress_interval) > 0;
}

/* create a compressed hypertable
 * table_relid - already created table which we are going to
 *               set up as a compressed hypertable
 * hypertable_id - id to be used while creating hypertable with
 *                  compression property set
 * NOTE:
 * compressed hypertable has no dimensions.
 */
bool
ts_hypertable_create_compressed(Oid table_relid, int32 hypertable_id)
{
	Oid user_oid = GetUserId();
	Oid tspc_oid = get_rel_tablespace(table_relid);
	NameData schema_name, table_name, associated_schema_name;
	ChunkSizingInfo *chunk_sizing_info;
	LockRelationOid(table_relid, AccessExclusiveLock);
	/*
	 * Check that the user has permissions to make this table to a compressed
	 * hypertable
	 */
	ts_hypertable_permissions_check(table_relid, user_oid);
	if (ts_is_hypertable(table_relid))
	{
		ereport(ERROR,
				(errcode(ERRCODE_TS_HYPERTABLE_EXISTS),
				 errmsg("table \"%s\" is already a hypertable", get_rel_name(table_relid))));
	}

	namestrcpy(&schema_name, get_namespace_name(get_rel_namespace(table_relid)));
	namestrcpy(&table_name, get_rel_name(table_relid));

	/* we don't use the chunking size info for managing the compressed table.
	 * But need this to satisfy hypertable constraints
	 */
	chunk_sizing_info = ts_chunk_sizing_info_get_default_disabled(table_relid);
	ts_chunk_sizing_func_validate(chunk_sizing_info->func, chunk_sizing_info);

	/* Checks pass, now we can create the catalog information */
	namestrcpy(&schema_name, get_namespace_name(get_rel_namespace(table_relid)));
	namestrcpy(&table_name, get_rel_name(table_relid));
	namestrcpy(&associated_schema_name, INTERNAL_SCHEMA_NAME);

	/* compressed hypertable has no dimensions of its own , shares the original hypertable dims*/
	hypertable_insert(hypertable_id,
					  &schema_name,
					  &table_name,
					  &associated_schema_name,
					  NULL,
					  &chunk_sizing_info->func_schema,
					  &chunk_sizing_info->func_name,
					  chunk_sizing_info->target_size_bytes,
					  0 /*num_dimensions*/,
					  true);

	/* No indexes are created for the compressed hypertable here */

	/* Attach tablespace, if any */
	if (OidIsValid(tspc_oid))
	{
		NameData tspc_name;

		namestrcpy(&tspc_name, get_tablespace_name(tspc_oid));
		ts_tablespace_attach_internal(&tspc_name, table_relid, false);
	}

	return true;
}

/*
 * Construct an expression for a dimensional column which is compatible with the max() function.
 * Normally, this is just the column name, but in the case of UUIDv7 there is no max() function
 * defined for the type so in that case the expression extracts the timestamp from the UUID.
 */
static const char *
get_expr_for_dim_max(const char *colname, Oid timetype)
{
	if (timetype == UUIDOID)
	{
		StringInfoData expr;

		initStringInfo(&expr);
		appendStringInfo(&expr,
						 "%s.uuid_timestamp(%s)",
						 ts_extension_schema_name(),
						 quote_identifier(colname));
		return expr.data;
	}

	return quote_identifier(colname);
}

/*
 * Get the max value of an open dimension.
 */
int64
ts_hypertable_get_open_dim_max_value(const Hypertable *ht, int dimension_index, bool *isnull)
{
	StringInfoData command;
	const Dimension *dim;
	int res;
	bool max_isnull;
	Datum maxdat;
	Oid timetype;

	dim = hyperspace_get_open_dimension(ht->space, dimension_index);

	if (NULL == dim)
		elog(ERROR, "invalid open dimension index %d", dimension_index);

	timetype = ts_dimension_get_partition_type(dim);

	/*
	 * Query for the last bucket in the materialized hypertable.
	 * Since this might be run as part of a parallel operation
	 * we cannot use SET search_path here to lock down the
	 * search_path and instead have to fully schema-qualify
	 * everything.
	 */
	initStringInfo(&command);
	appendStringInfo(&command,
					 "SELECT pg_catalog.max(%s) FROM %s.%s",
					 get_expr_for_dim_max(NameStr(dim->fd.column_name), timetype),
					 quote_identifier(NameStr(ht->fd.schema_name)),
					 quote_identifier(NameStr(ht->fd.table_name)));

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "could not connect to SPI");

	res = SPI_execute(command.data, true /* read_only */, 0 /*count*/);

	if (res < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 (errmsg("could not find the maximum time value for hypertable \"%s\"",
						 get_rel_name(ht->main_table_relid)))));

	/* In most cases the result type is the same as the time type. However, with UUIDs we first
	 * extract the timestamptz so the result type is timestamptz instead. */
	Oid result_type = timetype == UUIDOID ? TIMESTAMPTZOID : timetype;

	Ensure(SPI_gettypeid(SPI_tuptable->tupdesc, 1) == result_type,
		   "partition types for result (%d) and dimension (%d) do not match",
		   SPI_gettypeid(SPI_tuptable->tupdesc, 1),
		   ts_dimension_get_partition_type(dim));
	maxdat = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &max_isnull);

	if (isnull)
		*isnull = max_isnull;

	int64 max_value =
		max_isnull ? ts_time_get_min(result_type) : ts_time_value_to_internal(maxdat, result_type);

	res = SPI_finish();
	if (res != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed: %s", SPI_result_code_string(res));

	return max_value;
}

bool
ts_hypertable_has_compression_table(const Hypertable *ht)
{
	if (ht->fd.compressed_hypertable_id != INVALID_HYPERTABLE_ID)
	{
		Assert(ht->fd.compression_state == HypertableCompressionEnabled);
		return true;
	}
	return false;
}

/*
 * hypertable status update is done in two steps, similar to
 * chunk_update_status
 * This is again equivalent to a SELECT FOR UPDATE, followed by UPDATE
 * 1. RowShareLock to SELECT for UPDATE
 * 2. UPDATE status using RowExclusiveLock
 */
bool
ts_hypertable_update_status_osm(Hypertable *ht)
{
	FormData_hypertable form;
	ItemPointerData tid;
	/* lock the tuple entry in the catalog table */
	bool found = lock_hypertable_tuple(ht->fd.id, &tid, &form);
	Ensure(found, "hypertable id %d not found", ht->fd.id);

	if (form.status != ht->fd.status)
	{
		form.status = ht->fd.status;
		hypertable_update_catalog_tuple(&tid, &form);
	}
	return true;
}

bool
ts_hypertable_update_chunk_sizing(Hypertable *ht)
{
	FormData_hypertable form;
	ItemPointerData tid;
	/* lock the tuple entry in the catalog table */
	bool found = lock_hypertable_tuple(ht->fd.id, &tid, &form);
	Ensure(found, "hypertable id %d not found", ht->fd.id);

	if (OidIsValid(ht->chunk_sizing_func))
	{
		const Dimension *dim = ts_hyperspace_get_dimension(ht->space, DIMENSION_TYPE_OPEN, 0);
		ChunkSizingInfo info = {
			.table_relid = ht->main_table_relid,
			.colname = dim == NULL ? NULL : NameStr(dim->fd.column_name),
			.func = ht->chunk_sizing_func,
		};

		ts_chunk_adaptive_sizing_info_validate(&info);

		namestrcpy(&form.chunk_sizing_func_schema, NameStr(info.func_schema));
		namestrcpy(&form.chunk_sizing_func_name, NameStr(info.func_name));
	}
	else
		elog(ERROR, "chunk sizing function cannot be NULL");
	form.chunk_target_size = ht->fd.chunk_target_size;
	hypertable_update_catalog_tuple(&tid, &form);
	return true;
}

DimensionSlice *
ts_chunk_get_osm_slice_and_lock(int32 osm_chunk_id, int32 time_dim_id, LockTupleMode tuplockmode,
								LOCKMODE tablelockmode)
{
	ChunkConstraints *constraints =
		ts_chunk_constraint_scan_by_chunk_id(osm_chunk_id, 1, CurrentMemoryContext);

	for (int i = 0; i < constraints->num_constraints; i++)
	{
		ChunkConstraint *cc = chunk_constraints_get(constraints, i);
		if (is_dimension_constraint(cc))
		{
			ScanTupLock tuplock = {
				.lockmode = tuplockmode,
				.waitpolicy = LockWaitBlock,
			};
			/*
			 * We cannot acquire a tuple lock when running in recovery mode
			 * since that prevents scans on tiered hypertables from running
			 * on a read-only secondary. Acquiring a tuple lock requires
			 * assigning a transaction id for the current transaction state
			 * which is not possible in recovery mode. So we only acquire the
			 * lock if we are not in recovery mode.
			 */
			ScanTupLock *const tuplock_ptr = RecoveryInProgress() ? NULL : &tuplock;

			if (!IsolationUsesXactSnapshot())
			{
				/* in read committed mode, we follow all updates to this tuple */
				tuplock.lockflags |= TUPLE_LOCK_FLAG_FIND_LAST_VERSION;
			}

			DimensionSlice *dimslice =
				ts_dimension_slice_scan_by_id_and_lock(cc->fd.dimension_slice_id,
													   tuplock_ptr,
													   CurrentMemoryContext,
													   tablelockmode);
			if (dimslice->fd.dimension_id == time_dim_id)
				return dimslice;
		}
	}
	return NULL;
}
/*
 * hypertable_osm_range_update
 * 0 hypertable REGCLASS,
 * 1 range_start=NULL::bigint,
 * 2 range_end=NULL,
 * 3 empty=false
 * If empty is set to true then the range will be set to invalid range
 * but the overlap flag will be unset, indicating that no data is managed
 * by OSM and therefore timescaledb optimizations can be applied.
 */
TS_FUNCTION_INFO_V1(ts_hypertable_osm_range_update);
Datum
ts_hypertable_osm_range_update(PG_FUNCTION_ARGS)
{
	Oid relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	Hypertable *ht;
	const Dimension *time_dim;
	Cache *hcache;

	Oid time_type; /* required for resolving the argument types, should match the hypertable
					  partitioning column type */

	/*
	 * This function is not meant to be run on a read-only secondary. It is
	 * only used by OSM to update chunk's range in timescaledb catalog when
	 * tiering configuration changes (a new chunk is created, a chunk drop
	 * etc); OSM would already be holding a lock on a dimension slice tuple
	 * by this moment (which is not possible on read-only instance).
	 * Technically this function can be executed from SQL (e.g. from psql) when
	 * in recovery mode; in that instance an ERROR would be thrown when trying
	 * to update the dimension slice tuple, no harm will be done.
	 */
	Assert(!RecoveryInProgress());

	hcache = ts_hypertable_cache_pin();
	ht = ts_resolve_hypertable_from_table_or_cagg(hcache, relid, true);
	Assert(ht != NULL);
	time_dim = hyperspace_get_open_dimension(ht->space, 0);

	if (time_dim == NULL)
		ereport(ERROR,
				errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				errmsg("could not find time dimension for hypertable %s.%s",
					   quote_identifier(NameStr(ht->fd.schema_name)),
					   quote_identifier(NameStr(ht->fd.table_name))));

	time_type = ts_dimension_get_partition_type(time_dim);

	int32 osm_chunk_id = ts_chunk_get_osm_chunk_id(ht->fd.id);
	if (osm_chunk_id == INVALID_CHUNK_ID)
		ereport(ERROR,
				errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				errmsg("no OSM chunk found for hypertable %s.%s",
					   quote_identifier(NameStr(ht->fd.schema_name)),
					   quote_identifier(NameStr(ht->fd.table_name))));
	/*
	 * range_start, range_end arguments must be converted to internal representation
	 * a NULL start value is interpreted as INT64_MAX - 1 and a NULL end value is
	 * interpreted as INT64_MAX.
	 * Passing both start and end NULL values will reset the range to the default range an
	 * OSM chunk is given upon creation, which is [INT64_MAX - 1, INT64_MAX]
	 */
	if ((PG_ARGISNULL(1) && !PG_ARGISNULL(2)) || (!PG_ARGISNULL(1) && PG_ARGISNULL(2)))
		ereport(ERROR,
				errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("range_start and range_end parameters must be both NULL or both non-NULL"));

	Oid argtypes[2];
	for (int i = 0; i < 2; i++)
	{
		argtypes[i] = get_fn_expr_argtype(fcinfo->flinfo, i + 1);
		if (!can_coerce_type(1, &argtypes[i], &time_type, COERCION_IMPLICIT) &&
			!PG_ARGISNULL(i + 1))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid time argument type \"%s\"", format_type_be(argtypes[i])),
					 errhint("Try casting the argument to \"%s\".", format_type_be(time_type))));
	}

	int64 range_start_internal, range_end_internal;
	if (PG_ARGISNULL(1))
		range_start_internal = PG_INT64_MAX - 1;
	else
		range_start_internal =
			ts_time_value_to_internal(PG_GETARG_DATUM(1), get_fn_expr_argtype(fcinfo->flinfo, 1));
	if (PG_ARGISNULL(2))
		range_end_internal = PG_INT64_MAX;
	else
		range_end_internal =
			ts_time_value_to_internal(PG_GETARG_DATUM(2), get_fn_expr_argtype(fcinfo->flinfo, 2));

	if (range_start_internal > range_end_internal)
		ereport(ERROR,
				errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("dimension slice range_end cannot be less than range_start"));

	bool osm_chunk_empty = PG_GETARG_BOOL(3);

	bool overlap = false, range_invalid = false;

	/* Lock tuple FOR UPDATE */
	DimensionSlice *slice = ts_chunk_get_osm_slice_and_lock(osm_chunk_id,
															time_dim->fd.id,
															LockTupleExclusive,
															RowShareLock);

	if (!slice)
		ereport(ERROR, errmsg("could not find time dimension slice for chunk %d", osm_chunk_id));

	int32 dimension_slice_id = slice->fd.id;
	overlap = ts_osm_chunk_range_overlaps(dimension_slice_id,
										  slice->fd.dimension_id,
										  range_start_internal,
										  range_end_internal);
	/*
	 * It should not be possible for OSM chunks to overlap with the range
	 * managed by timescaledb. OSM extension should update the range of the
	 * OSM chunk to [INT64_MAX -1, infinity) when it detects that it is
	 * noncontiguous, so we should not end up detecting overlaps anyway.
	 * But throw an error in case we encounter this situation.
	 */
	if (overlap)
		ereport(ERROR,
				errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("attempting to set overlapping range for tiered chunk of %s.%s",
					   NameStr(ht->fd.schema_name),
					   NameStr(ht->fd.table_name)),
				errhint("Range should be set to invalid for tiered chunk"));
	range_invalid = ts_osm_chunk_range_is_invalid(range_start_internal, range_end_internal);
	/* Update the hypertable flags regarding the validity of the OSM range */
	if (range_invalid)
	{
		/* range is set to infinity so the OSM chunk is considered last */
		range_start_internal = PG_INT64_MAX - 1;
		range_end_internal = PG_INT64_MAX;
		if (!osm_chunk_empty)
			ht->fd.status =
				ts_set_flags_32(ht->fd.status, HYPERTABLE_STATUS_OSM_CHUNK_NONCONTIGUOUS);
		else
			ht->fd.status =
				ts_clear_flags_32(ht->fd.status, HYPERTABLE_STATUS_OSM_CHUNK_NONCONTIGUOUS);
	}
	else
		ht->fd.status = ts_clear_flags_32(ht->fd.status, HYPERTABLE_STATUS_OSM_CHUNK_NONCONTIGUOUS);

	ts_hypertable_update_status_osm(ht);
	ts_cache_release(&hcache);

	slice->fd.range_start = range_start_internal;
	slice->fd.range_end = range_end_internal;
	ts_dimension_slice_range_update(slice);

	PG_RETURN_BOOL(overlap);
}

TSDLLEXPORT bool
ts_hypertable_has_continuous_aggregates(int32 hypertable_id)
{
	bool found = false;
	ScanIterator iterator =
		ts_scan_iterator_create(CONTINUOUS_AGG, AccessShareLock, CurrentMemoryContext);

	iterator.ctx.limit = 1; /* we only need to know if there is at least one */
	iterator.ctx.index =
		catalog_get_index(ts_catalog_get(), CONTINUOUS_AGG, CONTINUOUS_AGG_RAW_HYPERTABLE_ID_IDX);
	ts_scan_iterator_scan_key_init(&iterator,
								   Anum_continuous_agg_raw_hypertable_id_idx_raw_hypertable_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(hypertable_id));

	ts_scan_iterator_start_scan(&iterator);
	if (ts_scan_iterator_next(&iterator))
		found = true;
	ts_scan_iterator_close(&iterator);

	return found;
}
