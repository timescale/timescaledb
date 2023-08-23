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
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <nodes/memnodes.h>
#include <nodes/parsenodes.h>
#include <nodes/value.h>
#include <parser/parse_func.h>
#include <storage/lmgr.h>
#include <utils/acl.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/snapmgr.h>
#include <utils/syscache.h>

#include "hypertable.h"
#include "ts_catalog/hypertable_data_node.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/metadata.h"
#include "hypercube.h"
#include "dimension.h"
#include "chunk.h"
#include "chunk_adaptive.h"
#include "ts_catalog/hypertable_compression.h"
#include "subspace_store.h"
#include "hypertable_cache.h"
#include "trigger.h"
#include "scanner.h"
#include "ts_catalog/catalog.h"
#include "dimension_slice.h"
#include "dimension_vector.h"
#include "hypercube.h"
#include "indexing.h"
#include "guc.h"
#include "errors.h"
#include "copy.h"
#include "utils.h"
#include "bgw_policy/policy.h"
#include "ts_catalog/continuous_agg.h"
#include "license_guc.h"
#include "cross_module_fn.h"
#include "scan_iterator.h"
#include "debug_assert.h"
#include "osm_callbacks.h"

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
	if (fd->replication_factor == 0)
		nulls[AttrNumberGetAttrOffset(Anum_hypertable_replication_factor)] = true;
	else
		values[AttrNumberGetAttrOffset(Anum_hypertable_replication_factor)] =
			Int16GetDatum(fd->replication_factor);

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

	fd->id = DatumGetInt32(values[AttrNumberGetAttrOffset(Anum_hypertable_id)]);
	memcpy(&fd->schema_name,
		   DatumGetName(values[AttrNumberGetAttrOffset(Anum_hypertable_schema_name)]),
		   NAMEDATALEN);
	memcpy(&fd->table_name,
		   DatumGetName(values[AttrNumberGetAttrOffset(Anum_hypertable_table_name)]),
		   NAMEDATALEN);
	memcpy(&fd->associated_schema_name,
		   DatumGetName(values[AttrNumberGetAttrOffset(Anum_hypertable_associated_schema_name)]),
		   NAMEDATALEN);
	memcpy(&fd->associated_table_prefix,
		   DatumGetName(values[AttrNumberGetAttrOffset(Anum_hypertable_associated_table_prefix)]),
		   NAMEDATALEN);

	fd->num_dimensions =
		DatumGetInt16(values[AttrNumberGetAttrOffset(Anum_hypertable_num_dimensions)]);

	memcpy(&fd->chunk_sizing_func_schema,
		   DatumGetName(values[AttrNumberGetAttrOffset(Anum_hypertable_chunk_sizing_func_schema)]),
		   NAMEDATALEN);
	memcpy(&fd->chunk_sizing_func_name,
		   DatumGetName(values[AttrNumberGetAttrOffset(Anum_hypertable_chunk_sizing_func_name)]),
		   NAMEDATALEN);

	fd->chunk_target_size =
		DatumGetInt64(values[AttrNumberGetAttrOffset(Anum_hypertable_chunk_target_size)]);
	fd->compression_state =
		DatumGetInt16(values[AttrNumberGetAttrOffset(Anum_hypertable_compression_state)]);

	if (nulls[AttrNumberGetAttrOffset(Anum_hypertable_compressed_hypertable_id)])
		fd->compressed_hypertable_id = INVALID_HYPERTABLE_ID;
	else
		fd->compressed_hypertable_id = DatumGetInt32(
			values[AttrNumberGetAttrOffset(Anum_hypertable_compressed_hypertable_id)]);
	if (nulls[AttrNumberGetAttrOffset(Anum_hypertable_replication_factor)])
		fd->replication_factor = INVALID_HYPERTABLE_ID;
	else
		fd->replication_factor =
			DatumGetInt16(values[AttrNumberGetAttrOffset(Anum_hypertable_replication_factor)]);

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
	h->data_nodes = ts_hypertable_data_node_scan(h->fd.id, ti->mctx);

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
					(errcode(ERRCODE_TS_INTERNAL_ERROR),
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
	int result = (ht == NULL) ? -1 : ht->fd.id;

	ts_cache_release(hcache);
	return result;
}

static bool
hypertable_is_compressed_or_materialization(const Hypertable *ht)
{
	ContinuousAggHypertableStatus status = ts_continuous_agg_hypertable_status(ht->fd.id);
	return (TS_HYPERTABLE_IS_INTERNAL_COMPRESSION_TABLE(ht) ||
			status == HypertableIsMaterialization);
}

static ScanFilterResult
hypertable_filter_exclude_compressed_and_materialization(const TupleInfo *ti, void *data)
{
	Hypertable *ht = ts_hypertable_from_tupleinfo(ti);

	return hypertable_is_compressed_or_materialization(ht) ? SCAN_EXCLUDE : SCAN_INCLUDE;
}

static int
hypertable_scan_limit_internal(ScanKeyData *scankey, int num_scankeys, int indexid,
							   tuple_found_func on_tuple_found, void *scandata, int limit,
							   LOCKMODE lock, bool tuplock, MemoryContext mctx,
							   tuple_filter_func filter)
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

static ScanTupleResult
hypertable_tuple_append(TupleInfo *ti, void *data)
{
	List **hypertables = data;

	*hypertables = lappend(*hypertables, ts_hypertable_from_tupleinfo(ti));

	return SCAN_CONTINUE;
}

List *
ts_hypertable_get_all(void)
{
	List *result = NIL;

	hypertable_scan_limit_internal(NULL,
								   0,
								   InvalidOid,
								   hypertable_tuple_append,
								   &result,
								   -1,
								   RowExclusiveLock,
								   false,
								   CurrentMemoryContext,
								   hypertable_filter_exclude_compressed_and_materialization);

	return result;
}

static ScanTupleResult
hypertable_tuple_update(TupleInfo *ti, void *data)
{
	Hypertable *ht = data;
	HeapTuple new_tuple;
	CatalogSecurityContext sec_ctx;

	if (OidIsValid(ht->chunk_sizing_func))
	{
		const Dimension *dim = ts_hyperspace_get_dimension(ht->space, DIMENSION_TYPE_OPEN, 0);
		ChunkSizingInfo info = {
			.table_relid = ht->main_table_relid,
			.colname = dim == NULL ? NULL : NameStr(dim->fd.column_name),
			.func = ht->chunk_sizing_func,
		};

		ts_chunk_adaptive_sizing_info_validate(&info);

		namestrcpy(&ht->fd.chunk_sizing_func_schema, NameStr(info.func_schema));
		namestrcpy(&ht->fd.chunk_sizing_func_name, NameStr(info.func_name));
	}
	else
		elog(ERROR, "chunk sizing function cannot be NULL");

	new_tuple = hypertable_formdata_make_tuple(&ht->fd, ts_scanner_get_tupledesc(ti));

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_update_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti), new_tuple);
	ts_catalog_restore_user(&sec_ctx);
	heap_freetuple(new_tuple);
	return SCAN_DONE;
}

int
ts_hypertable_update(Hypertable *ht)
{
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_hypertable_pkey_idx_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(ht->fd.id));

	return hypertable_scan_limit_internal(scankey,
										  1,
										  HYPERTABLE_ID_INDEX,
										  hypertable_tuple_update,
										  ht,
										  1,
										  RowExclusiveLock,
										  false,
										  CurrentMemoryContext,
										  NULL);
}

int
ts_hypertable_scan_with_memory_context(const char *schema, const char *table,
									   tuple_found_func tuple_found, void *data, LOCKMODE lockmode,
									   bool tuplock, MemoryContext mctx)
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
										  tuplock,
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
	ts_hypertable_data_node_delete_by_hypertable_id(hypertable_id);

	/* Also remove any policy argument / job that uses this hypertable */
	ts_bgw_policy_delete_by_hypertable_id(hypertable_id);

	/* Remove any dependent continuous aggs */
	ts_continuous_agg_drop_hypertable_callback(hypertable_id);

	/* remove any associated compression definitions */
	ts_hypertable_compression_delete_by_hypertable_id(hypertable_id);

	if (!compressed_hypertable_id_isnull)
	{
		Hypertable *compressed_hypertable = ts_hypertable_get_by_id(compressed_hypertable_id);
		/* The hypertable may have already been deleted by a cascade */
		if (compressed_hypertable != NULL)
			ts_hypertable_drop(compressed_hypertable, DROP_RESTRICT);
	}

#if PG14_GE
	OsmCallbacks *callbacks = ts_get_osm_callbacks();

	/* Invoke the OSM callback if set */
	if (callbacks)
	{
		Name schema_name =
			DatumGetName(slot_getattr(ti->slot, Anum_hypertable_schema_name, &isnull));
		Name table_name = DatumGetName(slot_getattr(ti->slot, Anum_hypertable_table_name, &isnull));

		callbacks->hypertable_drop_hook(NameStr(*schema_name), NameStr(*table_name));
	}
#endif

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
										  false,
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
										  false,
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
		performDeletion(&hypertable_addr, behavior, 0);
	}
	/* Clean up catalog */
	ts_hypertable_delete_by_name(hypertable->fd.schema_name.data, hypertable->fd.table_name.data);
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
										  false,
										  CurrentMemoryContext,
										  NULL);
}

static ScanTupleResult
tuple_found_lock(TupleInfo *ti, void *data)
{
	TM_Result *result = data;

	*result = ti->lockresult;
	return SCAN_DONE;
}

TM_Result
ts_hypertable_lock_tuple(Oid table_relid)
{
	TM_Result result;
	int num_found;

	num_found = hypertable_scan(get_namespace_name(get_rel_namespace(table_relid)),
								get_rel_name(table_relid),
								tuple_found_lock,
								&result,
								RowExclusiveLock,
								true);

	if (num_found != 1)
		ereport(ERROR,
				(errcode(ERRCODE_TS_HYPERTABLE_NOT_EXIST),
				 errmsg("table \"%s\" is not a hypertable", get_rel_name(table_relid))));

	return result;
}

bool
ts_hypertable_lock_tuple_simple(Oid table_relid)
{
	TM_Result result = ts_hypertable_lock_tuple(table_relid);

	switch (result)
	{
		case TM_SelfModified:

			/*
			 * Updated by the current transaction already. We equate this with
			 * a successful lock since the tuple should be locked if updated
			 * by us.
			 */
			return true;
		case TM_Ok:
			/* successfully locked */
			return true;

		case TM_Deleted:
		case TM_Updated:
			ereport(ERROR,
					(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
					 errmsg("hypertable \"%s\" has already been updated by another transaction",
							get_rel_name(table_relid)),
					 errhint("Retry the operation again.")));
			pg_unreachable();
			return false;

		case TM_BeingModified:
			ereport(ERROR,
					(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
					 errmsg("hypertable \"%s\" is being updated by another transaction",
							get_rel_name(table_relid)),
					 errhint("Retry the operation again.")));
			pg_unreachable();
			return false;
		case TM_WouldBlock:
			/* Locking would block. Let caller decide what to do */
			return false;
		case TM_Invisible:
			elog(ERROR, "attempted to lock invisible tuple");
			pg_unreachable();
			return false;
		default:
			elog(ERROR, "unexpected tuple lock status");
			pg_unreachable();
			return false;
	}
}

int
ts_hypertable_set_name(Hypertable *ht, const char *newname)
{
	namestrcpy(&ht->fd.table_name, newname);

	return ts_hypertable_update(ht);
}

int
ts_hypertable_set_schema(Hypertable *ht, const char *newname)
{
	namestrcpy(&ht->fd.schema_name, newname);

	return ts_hypertable_update(ht);
}

int
ts_hypertable_set_num_dimensions(Hypertable *ht, int16 num_dimensions)
{
	Assert(num_dimensions > 0);
	ht->fd.num_dimensions = num_dimensions;
	return ts_hypertable_update(ht);
}

#define DEFAULT_ASSOCIATED_TABLE_PREFIX_FORMAT "_hyper_%d"
#define DEFAULT_ASSOCIATED_DISTRIBUTED_TABLE_PREFIX_FORMAT "_dist_hyper_%d"
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
				  int64 chunk_target_size, int16 num_dimensions, bool compressed,
				  int16 replication_factor)
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
		Assert(replication_factor >= 0);
		if (replication_factor == 0)
			snprintf(NameStr(default_associated_table_prefix),
					 NAMEDATALEN,
					 DEFAULT_ASSOCIATED_TABLE_PREFIX_FORMAT,
					 fd.id);
		else
			snprintf(NameStr(default_associated_table_prefix),
					 NAMEDATALEN,
					 DEFAULT_ASSOCIATED_DISTRIBUTED_TABLE_PREFIX_FORMAT,
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

	/* finally, set replication factor */
	fd.replication_factor = replication_factor;

	rel = table_open(catalog_get_table_id(catalog, HYPERTABLE), RowExclusiveLock);
	hypertable_insert_relation(rel, &fd);
	table_close(rel, RowExclusiveLock);
}

static ScanTupleResult
hypertable_tuple_found(TupleInfo *ti, void *data)
{
	Hypertable **entry = data;

	*entry = ts_hypertable_from_tupleinfo(ti);
	return SCAN_DONE;
}

Hypertable *
ts_hypertable_get_by_name(const char *schema, const char *name)
{
	Hypertable *ht = NULL;

	hypertable_scan(schema, name, hypertable_tuple_found, &ht, AccessShareLock, false);

	return ht;
}

void
ts_hypertable_scan_by_name(ScanIterator *iterator, const char *schema, const char *name)
{
	iterator->ctx.index = catalog_get_index(ts_catalog_get(), HYPERTABLE, HYPERTABLE_NAME_INDEX);

	/* both cannot be NULL inputs */
	Assert(name != NULL || schema != NULL);

	if (name)
		ts_scan_iterator_scan_key_init(iterator,
									   Anum_hypertable_name_idx_table,
									   BTEqualStrategyNumber,
									   F_NAMEEQ,
									   CStringGetDatum(name));

	if (schema)
		ts_scan_iterator_scan_key_init(iterator,
									   Anum_hypertable_name_idx_schema,
									   BTEqualStrategyNumber,
									   F_NAMEEQ,
									   CStringGetDatum(schema));
}

/*
 * Find hypertable by name and retrieve catalog form attributes.
 *
 * In case if some of the requested attributes marked as NULL value, nulls[] array will be
 * modified and appropriate attribute position bit will be set to true.
 *
 * Return true if hypertable is found, false otherwise.
 */
bool
ts_hypertable_get_attributes_by_name(const char *schema, const char *name,
									 FormData_hypertable *form)
{
	ScanIterator iterator =
		ts_scan_iterator_create(HYPERTABLE, AccessShareLock, CurrentMemoryContext);

	ts_hypertable_scan_by_name(&iterator, schema, name);
	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		ts_hypertable_formdata_fill(form, ti);
		ts_scan_iterator_close(&iterator);
		return true;
	}

	return false;
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
								   &ht,
								   1,
								   AccessShareLock,
								   false,
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
static Chunk *
hypertable_chunk_store_add(const Hypertable *h, const Chunk *input_chunk)
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
 */
Chunk *
ts_hypertable_create_chunk_for_point(const Hypertable *h, const Point *point, bool *found)
{
	Assert(ts_subspace_store_get(h->chunk_cache, point) == NULL);

	Chunk *chunk = ts_chunk_create_for_point(h,
											 point,
											 found,
											 NameStr(h->fd.associated_schema_name),
											 NameStr(h->fd.associated_table_prefix));

	/* Also add the chunk to the hypertable's chunk store */
	Chunk *cached_chunk = hypertable_chunk_store_add(h, chunk);
	return cached_chunk;
}

/*
 * Find the chunk containing the given point, locking all its dimension slices
 * for share. NULL if not found.
 * Also uses hypertable chunk cache. The returned chunk is owned by the cache
 * and may become invalid after some subsequent call to this function.
 * Leaks memory, so call in a short-lived context.
 */
Chunk *
ts_hypertable_find_chunk_for_point(const Hypertable *h, const Point *point)
{
	Chunk *chunk = ts_subspace_store_get(h->chunk_cache, point);
	if (chunk != NULL)
	{
		return chunk;
	}

	chunk = ts_chunk_find_for_point(h, point);
	if (chunk == NULL)
	{
		return NULL;
	}

	/* Also add the chunk to the hypertable's chunk store */
	Chunk *cached_chunk = hypertable_chunk_store_add(h, chunk);
	return cached_chunk;
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

	ts_cache_release(hcache);

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

static bool
relation_has_tuples(Relation rel)
{
	TableScanDesc scandesc = table_beginscan(rel, GetActiveSnapshot(), 0, NULL);
	TupleTableSlot *slot =
		MakeSingleTupleTableSlot(RelationGetDescr(rel), table_slot_callbacks(rel));
	bool hastuples = table_scan_getnextslot(scandesc, ForwardScanDirection, slot);

	table_endscan(scandesc);
	ExecDropSingleTupleTableSlot(slot);
	return hastuples;
}

static bool
table_has_tuples(Oid table_relid, LOCKMODE lockmode)
{
	Relation rel = table_open(table_relid, lockmode);
	bool hastuples = relation_has_tuples(rel);

	table_close(rel, lockmode);
	return hastuples;
}

static bool
table_is_logged(Oid table_relid)
{
	return get_rel_persistence(table_relid) == RELPERSISTENCE_PERMANENT;
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
 * Hypertables do not support some constraints. For instance, NO INHERIT
 * constraints cannot be enforced on a hypertable since they only exist on the
 * parent table, which will have no tuples.
 */
static void
hypertable_validate_constraints(Oid relid, int replication_factor)
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

		if (form->contype == CONSTRAINT_CHECK && form->connoinherit)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("cannot have NO INHERIT constraints on hypertable \"%s\"",
							get_rel_name(relid)),
					 errhint("Remove all NO INHERIT constraints from table \"%s\" before "
							 "making it a hypertable.",
							 get_rel_name(relid))));

		if (form->contype == CONSTRAINT_FOREIGN && replication_factor > 0)
			ereport(WARNING,
					(errmsg("distributed hypertable \"%s\" has a foreign key to"
							" a non-distributed table",
							get_rel_name(relid)),
					 errdetail("Non-distributed tables that are referenced by a distributed"
							   " hypertable must exist and be identical on all data nodes.")));
	}

	systable_endscan(scan);
	table_close(catalog, AccessShareLock);
}

/*
 * Functionality to block INSERTs on the hypertable's root table.
 *
 * The design considered implementing this either with RULES, constraints, or
 * triggers. A visible trigger was found to have the best trade-offs:
 *
 * - A RULE doesn't work since it rewrites the query and thus blocks INSERTs
 *   also on the hypertable.
 *
 * - A constraint is not transparent, i.e., viewing the hypertable with \d+
 *   <table> would list the constraint and that breaks the abstraction of
 *   "hypertables being like regular tables." Further, a constraint remains on
 *   the table after the extension is dropped, which prohibits running
 *   create_hypertable() on the same table once the extension is created again
 *   (you can work around this, but is messy). This issue, b.t.w., broke one
 *   of the tests.
 *
 * - An internal trigger is transparent (doesn't show up
 *	 on \d+ <table>) and is automatically removed when the extension is
 *	 dropped (since it is part of the extension). Internal triggers aren't
 *	 inherited by chunks either, so we need no special handling to _not_
 *	 inherit the blocking trigger. However, internal triggers are not exported
 *   via pg_dump. Because a critical use case for this trigger is to ensure
 *   no rows are inserted into hypertables by accident when a user forgets to
 *   turn restoring off, having this trigger exported in pg_dump is essential.
 *
 * - A visible trigger unfortunately shows up in \d+ <table>, but is
 *   included in a pg_dump. We also add logic to make sure this trigger is not
 *   propagated to chunks.
 */
TS_FUNCTION_INFO_V1(ts_hypertable_insert_blocker);

Datum
ts_hypertable_insert_blocker(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	const char *relname = get_rel_name(trigdata->tg_relation->rd_id);

	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "insert_blocker: not called by trigger manager");

	if (ts_guc_restoring)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot INSERT into hypertable \"%s\" during restore", relname),
				 errhint("Set 'timescaledb.restoring' to 'off' after the restore process has "
						 "finished.")));
	else
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("invalid INSERT on the root table of hypertable \"%s\"", relname),
				 errhint("Make sure the TimescaleDB extension has been preloaded.")));

	PG_RETURN_NULL();
}

/*
 * Get the legacy insert blocker trigger on a table.
 *
 * Note that we cannot get the old insert trigger by name since internal triggers
 * are made unique by appending the trigger OID, which we do not
 * know. Instead, we have to search all triggers.
 */
static Oid
old_insert_blocker_trigger_get(Oid relid)
{
	Relation tgrel;
	ScanKeyData skey[1];
	SysScanDesc tgscan;
	HeapTuple tuple;
	Oid tgoid = InvalidOid;

	tgrel = table_open(TriggerRelationId, AccessShareLock);

	ScanKeyInit(&skey[0],
				Anum_pg_trigger_tgrelid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(relid));

	tgscan = systable_beginscan(tgrel, TriggerRelidNameIndexId, true, NULL, 1, skey);

	while (HeapTupleIsValid(tuple = systable_getnext(tgscan)))
	{
		Form_pg_trigger trig = (Form_pg_trigger) GETSTRUCT(tuple);

		if (TRIGGER_TYPE_MATCHES(trig->tgtype,
								 TRIGGER_TYPE_ROW,
								 TRIGGER_TYPE_BEFORE,
								 TRIGGER_TYPE_INSERT) &&
			strncmp(OLD_INSERT_BLOCKER_NAME,
					NameStr(trig->tgname),
					strlen(OLD_INSERT_BLOCKER_NAME)) == 0 &&
			trig->tgisinternal)
		{
			tgoid = trig->oid;
			break;
		}
	}

	systable_endscan(tgscan);
	table_close(tgrel, AccessShareLock);

	return tgoid;
}

/*
 * Add an INSERT blocking trigger to a table.
 *
 * The blocking trigger is used to block accidental INSERTs on a hypertable's
 * root table.
 */
static Oid
insert_blocker_trigger_add(Oid relid)
{
	ObjectAddress objaddr;
	char *relname = get_rel_name(relid);
	Oid schemaid = get_rel_namespace(relid);
	char *schema = get_namespace_name(schemaid);
	CreateTrigStmt stmt = {
		.type = T_CreateTrigStmt,
		.row = true,
		.timing = TRIGGER_TYPE_BEFORE,
		.trigname = INSERT_BLOCKER_NAME,
		.relation = makeRangeVar(schema, relname, -1),
		.funcname =
			list_make2(makeString(FUNCTIONS_SCHEMA_NAME), makeString(OLD_INSERT_BLOCKER_NAME)),
		.args = NIL,
		.events = TRIGGER_TYPE_INSERT,
	};

	/*
	 * We create a user-visible trigger, so that it will get pg_dump'd with
	 * the hypertable. This call will error out if a trigger with the same
	 * name already exists. (This is the desired behavior.)
	 */
	objaddr = CreateTrigger(&stmt,
							NULL,
							relid,
							InvalidOid,
							InvalidOid,
							InvalidOid,
							InvalidOid,
							InvalidOid,
							NULL,
							false,
							false);

	if (!OidIsValid(objaddr.objectId))
		elog(ERROR, "could not create insert blocker trigger");

	return objaddr.objectId;
}

TS_FUNCTION_INFO_V1(ts_hypertable_insert_blocker_trigger_add);

/*
 * This function is exposed to drop the old blocking trigger on legacy hypertables.
 * We can't do it from SQL code, because internal triggers cannot be dropped from SQL.
 * After the legacy internal trigger is dropped, we add the new, visible trigger.
 *
 * In case the hypertable's root table has data in it, we bail out with an
 * error instructing the user to fix the issue first.
 */
Datum
ts_hypertable_insert_blocker_trigger_add(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	Oid old_trigger;

	ts_hypertable_permissions_check(relid, GetUserId());

	if (table_has_tuples(relid, AccessShareLock))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("hypertable \"%s\" has data in the root table", get_rel_name(relid)),
				 errdetail("Migrate the data from the root table to chunks before running the "
						   "UPDATE again."),
				 errhint("Data can be migrated as follows:\n"
						 "> BEGIN;\n"
						 "> SET timescaledb.restoring = 'off';\n"
						 "> INSERT INTO \"%1$s\" SELECT * FROM ONLY \"%1$s\";\n"
						 "> SET timescaledb.restoring = 'on';\n"
						 "> TRUNCATE ONLY \"%1$s\";\n"
						 "> SET timescaledb.restoring = 'off';\n"
						 "> COMMIT;",
						 get_rel_name(relid))));

	/* Now drop the old trigger */
	old_trigger = old_insert_blocker_trigger_get(relid);
	if (OidIsValid(old_trigger))
	{
		ObjectAddress objaddr = { .classId = TriggerRelationId, .objectId = old_trigger };

		performDeletion(&objaddr, DROP_RESTRICT, 0);
	}

	/* Add the new trigger */
	PG_RETURN_OID(insert_blocker_trigger_add(relid));
}

static Datum
create_hypertable_datum(FunctionCallInfo fcinfo, const Hypertable *ht, bool created)
{
	TupleDesc tupdesc;
	Datum values[Natts_create_hypertable];
	bool nulls[Natts_create_hypertable] = { false };
	HeapTuple tuple;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in "
						"context that cannot accept type record")));

	tupdesc = BlessTupleDesc(tupdesc);
	values[AttrNumberGetAttrOffset(Anum_create_hypertable_id)] = Int32GetDatum(ht->fd.id);
	values[AttrNumberGetAttrOffset(Anum_create_hypertable_schema_name)] =
		NameGetDatum(&ht->fd.schema_name);
	values[AttrNumberGetAttrOffset(Anum_create_hypertable_table_name)] =
		NameGetDatum(&ht->fd.table_name);
	values[AttrNumberGetAttrOffset(Anum_create_hypertable_created)] = BoolGetDatum(created);
	tuple = heap_form_tuple(tupdesc, values, nulls);

	return HeapTupleGetDatum(tuple);
}

/*
 * Check that the partitioning is reasonable and raise warnings if
 * not. Typically called after applying updates to a partitioning dimension.
 */
void
ts_hypertable_check_partitioning(const Hypertable *ht, int32 id_of_updated_dimension)
{
	const Dimension *dim;

	Assert(OidIsValid(id_of_updated_dimension));

	dim = ts_hyperspace_get_dimension_by_id(ht->space, id_of_updated_dimension);

	Assert(dim);

	if (hypertable_is_distributed(ht))
	{
		const Dimension *first_closed_dim = hyperspace_get_closed_dimension(ht->space, 0);
		int num_nodes = list_length(ht->data_nodes);

		/* Warn the user that there aren't enough slices to make use of all
		 * servers. Only do this if this is the first closed (space) dimension. */
		if (first_closed_dim != NULL && dim->fd.id == first_closed_dim->fd.id &&
			num_nodes > first_closed_dim->fd.num_slices)
			ereport(WARNING,
					(errcode(ERRCODE_WARNING),
					 errmsg("insufficient number of partitions for dimension \"%s\"",
							NameStr(dim->fd.column_name)),
					 errdetail("There are not enough partitions to make"
							   " use of all data nodes."),
					 errhint("Increase the number of partitions in dimension \"%s\" to match or"
							 " exceed the number of attached data nodes.",
							 NameStr(dim->fd.column_name))));
	}
}

extern int16
ts_validate_replication_factor(const char *hypertable_name, int32 replication_factor,
							   int num_data_nodes)
{
	bool valid = replication_factor >= 1 && replication_factor <= PG_INT16_MAX;

	if (num_data_nodes < replication_factor)
		ereport(ERROR,
				(errcode(ERRCODE_TS_INSUFFICIENT_NUM_DATA_NODES),
				 errmsg("replication factor too large for hypertable \"%s\"", hypertable_name),
				 errdetail("The hypertable has %d data nodes attached, while "
						   "the replication factor is %d.",
						   num_data_nodes,
						   replication_factor),
				 errhint("Decrease the replication factor or attach more data "
						 "nodes to the hypertable.")));

	if (!valid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid replication factor"),
				 errhint("A hypertable's replication factor must be between 1 and %d.",
						 PG_INT16_MAX)));

	return (int16) (replication_factor & 0xFFFF);
}

static int16
hypertable_validate_create_call(const char *hypertable_name, bool distributed,
								bool distributed_is_null, int32 replication_factor,
								bool replication_factor_is_null, ArrayType *data_node_arr,
								List **data_nodes)
{
	bool distributed_local_error = false;

	if (!distributed_is_null && !replication_factor_is_null)
	{
		/* create_hypertable(distributed, replication_factor) */
		if (!distributed)
			distributed_local_error = true;
	}
	else if (!distributed_is_null)
	{
		/* create_hypertable(distributed) */
		switch (ts_guc_hypertable_distributed_default)
		{
			case HYPERTABLE_DIST_AUTO:
			case HYPERTABLE_DIST_DISTRIBUTED:
				if (distributed)
					replication_factor = ts_guc_hypertable_replication_factor_default;
				else
				{
					/* creating regular hypertable according to the policy */
				}
				break;
			case HYPERTABLE_DIST_LOCAL:
				if (distributed)
					replication_factor = ts_guc_hypertable_replication_factor_default;
				else
					distributed_local_error = true;
				break;
		}
	}
	else if (!replication_factor_is_null)
	{
		/* create_hypertable(replication_factor) */
		switch (ts_guc_hypertable_distributed_default)
		{
			case HYPERTABLE_DIST_AUTO:
			case HYPERTABLE_DIST_DISTRIBUTED:
				/* distributed and distributed member */
				distributed = true;
				break;
			case HYPERTABLE_DIST_LOCAL:
				distributed_local_error = true;
				break;
		}
	}
	else
	{
		/* create_hypertable() */
		switch (ts_guc_hypertable_distributed_default)
		{
			case HYPERTABLE_DIST_AUTO:
				distributed = false;
				break;
			case HYPERTABLE_DIST_LOCAL:
				distributed_local_error = true;
				break;
			case HYPERTABLE_DIST_DISTRIBUTED:
				replication_factor = ts_guc_hypertable_replication_factor_default;
				distributed = true;
				break;
		}
	}

	if (distributed_local_error)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("local hypertables cannot set replication_factor"),
				 errhint("Set distributed=>true or use create_distributed_hypertable() to create a "
						 "distributed hypertable.")));

	if (!distributed)
		return 0;

	/*
	 * Special replication_factor case for hypertables created on remote
	 * data nodes. Used to distinguish them from regular hypertables.
	 *
	 * Such argument is only allowed to be use by access node session.
	 */
	if (replication_factor == -1)
	{
		bool an_session_on_dn =
			ts_cm_functions->is_access_node_session && ts_cm_functions->is_access_node_session();
		if (an_session_on_dn)
			return -1;
	}

	/* Validate data nodes and check permissions on them */
	if (replication_factor > 0)
		*data_nodes = ts_cm_functions->get_and_validate_data_node_list(data_node_arr);

	/* Check replication_factor value and the number of nodes */
	return ts_validate_replication_factor(hypertable_name,
										  replication_factor,
										  list_length(*data_nodes));
}

TS_FUNCTION_INFO_V1(ts_hypertable_create);
TS_FUNCTION_INFO_V1(ts_hypertable_distributed_create);

/*
 * Create a hypertable from an existing table.
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
 * replication_factor      INTEGER = NULL
 * data nodes              NAME[] = NULL
 * distributed             BOOLEAN = NULL (not present for dist call)
 */
static Datum
ts_hypertable_create_internal(PG_FUNCTION_ARGS, bool is_dist_call)
{
	Oid table_relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	Name time_dim_name = PG_ARGISNULL(1) ? NULL : PG_GETARG_NAME(1);
	Name space_dim_name = PG_ARGISNULL(2) ? NULL : PG_GETARG_NAME(2);
	Name associated_schema_name = PG_ARGISNULL(4) ? NULL : PG_GETARG_NAME(4);
	Name associated_table_prefix = PG_ARGISNULL(5) ? NULL : PG_GETARG_NAME(5);
	bool create_default_indexes =
		PG_ARGISNULL(7) ? false : PG_GETARG_BOOL(7); /* Defaults to true in the sql code */
	bool if_not_exists = PG_ARGISNULL(8) ? false : PG_GETARG_BOOL(8);
	bool migrate_data = PG_ARGISNULL(10) ? false : PG_GETARG_BOOL(10);
	DimensionInfo *time_dim_info =
		ts_dimension_info_create_open(table_relid,
									  /* column name */
									  time_dim_name,
									  /* interval */
									  PG_ARGISNULL(6) ? Int64GetDatum(-1) : PG_GETARG_DATUM(6),
									  /* interval type */
									  PG_ARGISNULL(6) ? InvalidOid :
														get_fn_expr_argtype(fcinfo->flinfo, 6),
									  /* partitioning func */
									  PG_ARGISNULL(13) ? InvalidOid : PG_GETARG_OID(13));
	DimensionInfo *space_dim_info = NULL;
	bool replication_factor_is_null = PG_ARGISNULL(14);
	int32 replication_factor_in = replication_factor_is_null ? 0 : PG_GETARG_INT32(14);
	int16 replication_factor;
	ArrayType *data_node_arr = PG_ARGISNULL(15) ? NULL : PG_GETARG_ARRAYTYPE_P(15);
	ChunkSizingInfo chunk_sizing_info = {
		.table_relid = table_relid,
		.target_size = PG_ARGISNULL(11) ? NULL : PG_GETARG_TEXT_P(11),
		.func = PG_ARGISNULL(12) ? InvalidOid : PG_GETARG_OID(12),
		.colname = PG_ARGISNULL(1) ? NULL : PG_GETARG_CSTRING(1),
		.check_for_index = !create_default_indexes,
	};
	bool distributed_is_null;
	bool distributed;

	/* create_distributed_hypertable() does not have explicit
	 * distributed argument */
	if (!is_dist_call)
	{
		distributed_is_null = PG_ARGISNULL(16);
		distributed = distributed_is_null ? false : PG_GETARG_BOOL(16);
	}
	else
	{
		distributed_is_null = false;
		distributed = true;
	}

	Cache *hcache;
	Hypertable *ht;
	Datum retval;
	bool created;
	uint32 flags = 0;
	List *data_nodes = NIL;

	ts_feature_flag_check(FEATURE_HYPERTABLE);

	TS_PREVENT_FUNC_IF_READ_ONLY();

	if (!OidIsValid(table_relid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("relation cannot be NULL")));

	if (migrate_data && is_dist_call)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot migrate data for distributed hypertable")));

	if (NULL == time_dim_name)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("time column cannot be NULL")));

	if (NULL != data_node_arr && ARR_NDIM(data_node_arr) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid data nodes format"),
				 errhint("Specify a one-dimensional array of data nodes.")));

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
		ts_cache_release(hcache);

		/*
		 * Validate create_hypertable arguments and use defaults accoring to the
		 * hypertable_distributed_default guc.
		 *
		 * Validate data nodes and check permissions on them if this is a
		 * distributed hypertable.
		 */
		replication_factor = hypertable_validate_create_call(get_rel_name(table_relid),
															 distributed,
															 distributed_is_null,
															 replication_factor_in,
															 replication_factor_is_null,
															 data_node_arr,
															 &data_nodes);

		if (NULL != space_dim_name)
		{
			int16 num_partitions = PG_ARGISNULL(3) ? -1 : PG_GETARG_INT16(3);

			/* If the number of partitions isn't specified, default to setting it
			 * to the number of data nodes */
			if (num_partitions < 1 && replication_factor > 0)
			{
				int num_nodes = list_length(data_nodes);

				Assert(num_nodes >= 0);
				num_partitions = num_nodes & 0xFFFF;
			}

			space_dim_info =
				ts_dimension_info_create_closed(table_relid,
												/* column name */
												space_dim_name,
												/* number partitions */
												num_partitions,
												/* partitioning func */
												PG_ARGISNULL(9) ? InvalidOid : PG_GETARG_OID(9));
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
												 time_dim_info,
												 space_dim_info,
												 associated_schema_name,
												 associated_table_prefix,
												 &chunk_sizing_info,
												 replication_factor,
												 data_nodes);

		Assert(created);
		ht = ts_hypertable_cache_get_cache_and_entry(table_relid, CACHE_FLAG_NONE, &hcache);
		if (NULL != space_dim_info)
			ts_hypertable_check_partitioning(ht, space_dim_info->dimension_id);
	}

	retval = create_hypertable_datum(fcinfo, ht, created);
	ts_cache_release(hcache);

	PG_RETURN_DATUM(retval);
}

Datum
ts_hypertable_create(PG_FUNCTION_ARGS)
{
	return ts_hypertable_create_internal(fcinfo, false);
}

Datum
ts_hypertable_distributed_create(PG_FUNCTION_ARGS)
{
	return ts_hypertable_create_internal(fcinfo, true);
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
							   DimensionInfo *time_dim_info, DimensionInfo *space_dim_info,
							   Name associated_schema_name, Name associated_table_prefix,
							   ChunkSizingInfo *chunk_sizing_info, int16 replication_factor,
							   List *data_node_names)
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
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("table \"%s\" is already partitioned", get_rel_name(table_relid)),
					 errdetail("It is not possible to turn partitioned tables into hypertables.")));
		case RELKIND_MATVIEW:
		case RELKIND_RELATION:
			break;

		default:
			ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("invalid relation type")));
	}

	/* Check that the table doesn't have any unsupported constraints */
	hypertable_validate_constraints(table_relid, replication_factor);

	table_has_data = relation_has_tuples(rel);

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

	if (!table_is_logged(table_relid))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("table \"%s\" has to be logged", get_rel_name(table_relid)),
				 errdetail(
					 "It is not possible to turn temporary or unlogged tables into hypertables.")));

	if (table_has_rules(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("hypertables do not support rules"),
				 errdetail("Table \"%s\" has attached rules, which do not work on hypertables.",
						   get_rel_name(table_relid)),
				 errhint("Remove the rules before creating a hypertable.")));

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
	 * Hypertables do not support transition tables in triggers, so if the
	 * table already has such triggers we bail out
	 */
	if (ts_relation_has_transition_table_trigger(table_relid))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("hypertables do not support transition tables in triggers")));

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

	if (DIMENSION_INFO_IS_SET(space_dim_info))
		ts_dimension_info_validate(space_dim_info);

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
					  DIMENSION_INFO_IS_SET(space_dim_info) ? 2 : 1,
					  false,
					  replication_factor);

	/* Get the a Hypertable object via the cache */
	time_dim_info->ht =
		ts_hypertable_cache_get_cache_and_entry(table_relid, CACHE_FLAG_NONE, &hcache);

	/* Add validated dimensions */
	ts_dimension_add_from_info(time_dim_info);

	if (DIMENSION_INFO_IS_SET(space_dim_info))
	{
		space_dim_info->ht = time_dim_info->ht;
		ts_dimension_add_from_info(space_dim_info);
		ts_dimension_partition_info_recreate(space_dim_info->dimension_id,
											 space_dim_info->num_slices,
											 data_node_names,
											 replication_factor);
	}

	/* Refresh the cache to get the updated hypertable with added dimensions */
	ts_cache_release(hcache);
	ht = ts_hypertable_cache_get_cache_and_entry(table_relid, CACHE_FLAG_NONE, &hcache);

	/* Verify that existing indexes are compatible with a hypertable */
	ts_indexing_verify_indexes(ht);

	/* Attach tablespace, if any */
	if (OidIsValid(tspc_oid) && !hypertable_is_distributed(ht))
	{
		NameData tspc_name;

		namestrcpy(&tspc_name, get_tablespace_name(tspc_oid));
		ts_tablespace_attach_internal(&tspc_name, table_relid, false);
	}

	/*
	 * Migrate data from the main table to chunks
	 *
	 * Note: we do not unlock here. We wait till the end of the txn instead.
	 * Must close the relation before migrating data.
	 */
	table_close(rel, NoLock);

	if (table_has_data)
	{
		ereport(NOTICE,
				(errmsg("migrating data to chunks"),
				 errdetail("Migration might take a while depending on the amount of data.")));

		timescaledb_move_from_table_to_chunks(ht, RowExclusiveLock);
	}

	insert_blocker_trigger_add(table_relid);

	if ((flags & HYPERTABLE_CREATE_DISABLE_DEFAULT_INDEXES) == 0)
		ts_indexing_create_default_indexes(ht);

	if (replication_factor > 0)
		ts_cm_functions->hypertable_make_distributed(ht, data_node_names);
	else if (list_length(data_node_names) > 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid replication factor"),
				 errhint("The replication factor should be 1 or greater with a non-empty data node "
						 "list.")));

	ts_cache_release(hcache);

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

typedef struct AccumHypertable
{
	List *ht_oids;
	Name schema_name;
	Name table_name;
} AccumHypertable;

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
		ReleaseSysCache(tuple);
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
				 errmsg("custom time function not supported on internal compression table")));

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
						NULL,
						&now_func_oid);
	ts_hypertable_func_call_on_data_nodes(hypertable, fcinfo);
	ts_cache_release(hcache);
	PG_RETURN_NULL();
}

/*Assume permissions are already checked
 * set compression state as enabled
 */
bool
ts_hypertable_set_compressed(Hypertable *ht, int32 compressed_hypertable_id)
{
	Assert(!TS_HYPERTABLE_IS_INTERNAL_COMPRESSION_TABLE(ht));
	ht->fd.compression_state = HypertableCompressionEnabled;
	/* distr. hypertables do not have a internal compression table
	 * on the access node
	 */
	if (!hypertable_is_distributed(ht))
		ht->fd.compressed_hypertable_id = compressed_hypertable_id;
	return ts_hypertable_update(ht) > 0;
}

/* set compression_state as disabled and remove any
 * associated compressed hypertable id
 */
bool
ts_hypertable_unset_compressed(Hypertable *ht)
{
	Assert(!TS_HYPERTABLE_IS_INTERNAL_COMPRESSION_TABLE(ht));
	ht->fd.compression_state = HypertableCompressionOff;
	ht->fd.compressed_hypertable_id = INVALID_HYPERTABLE_ID;
	return ts_hypertable_update(ht) > 0;
}

bool
ts_hypertable_set_compress_interval(Hypertable *ht, int64 compress_interval)
{
	Assert(!TS_HYPERTABLE_IS_INTERNAL_COMPRESSION_TABLE(ht));
	Assert(TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(ht));

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
	Relation rel;
	rel = table_open(table_relid, AccessExclusiveLock);
	Size row_size = MAXALIGN(SizeofHeapTupleHeader);
	/* estimate tuple width of compressed hypertable */
	for (int i = 1; i <= RelationGetNumberOfAttributes(rel); i++)
	{
		bool is_varlena = false;
		Oid outfunc;
		Form_pg_attribute att = TupleDescAttr(rel->rd_att, i - 1);
		getTypeOutputInfo(att->atttypid, &outfunc, &is_varlena);
		if (is_varlena)
			row_size += 18;
		else
			row_size += att->attlen;
	}
	if (row_size > MaxHeapTupleSize)
	{
		ereport(WARNING,
				(errmsg("compressed row size might exceed maximum row size"),
				 errdetail("Estimated row size of compressed hypertable is %zu. This exceeds the "
						   "maximum size of %zu and can cause compression of chunks to fail.",
						   row_size,
						   MaxHeapTupleSize)));
	}
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
		table_close(rel, AccessExclusiveLock);
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
					  true,
					  0 /* replication factor */);

	/* No indexes are created for the compressed hypertable here */

	/* Attach tablespace, if any */
	if (OidIsValid(tspc_oid))
	{
		NameData tspc_name;

		namestrcpy(&tspc_name, get_tablespace_name(tspc_oid));
		ts_tablespace_attach_internal(&tspc_name, table_relid, false);
	}

	insert_blocker_trigger_add(table_relid);
	/* lock will be released after the transaction is done */
	table_close(rel, NoLock);
	return true;
}

TSDLLEXPORT void
ts_hypertable_clone_constraints_to_compressed(const Hypertable *user_ht, List *constraint_list)
{
	CatalogSecurityContext sec_ctx;

	ListCell *lc;
	Assert(TS_HYPERTABLE_HAS_COMPRESSION_TABLE(user_ht));
	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	foreach (lc, constraint_list)
	{
		NameData *conname = lfirst(lc);
		CatalogInternalCall4(DDL_ADD_HYPERTABLE_FK_CONSTRAINT,
							 NameGetDatum(conname),
							 NameGetDatum(&user_ht->fd.schema_name),
							 NameGetDatum(&user_ht->fd.table_name),
							 Int32GetDatum(user_ht->fd.compressed_hypertable_id));
	}
	ts_catalog_restore_user(&sec_ctx);
}

#if defined(USE_ASSERT_CHECKING)
static void
assert_chunk_data_nodes_is_a_set(const List *chunk_data_nodes)
{
	Bitmapset *chunk_data_node_oids = NULL;
	ListCell *lc;

	foreach (lc, chunk_data_nodes)
	{
		const char *nodename = lfirst(lc);
		const ForeignServer *server = GetForeignServerByName(nodename, false);
		chunk_data_node_oids = bms_add_member(chunk_data_node_oids, server->serverid);
	}

	Assert(list_length(chunk_data_nodes) == bms_num_members(chunk_data_node_oids));
}
#endif

/*
 * Assign data nodes to a chunk.
 *
 * For space-partitioned tables, the first "closed" space dimension should
 * have explicit partitioning information to map the chunk to a set of data
 * nodes (dimension partition metadata). Otherwise compute the set of data
 * nodes dynamically.
 *
 * A chunk should be assigned up to replication_factor number of data
 * nodes. Dynamic assignment happens similar to tablespaces, i.e., based on
 * dimension type.
 *
 * Returns a list of data node name strings (char *).
 */
List *
ts_hypertable_assign_chunk_data_nodes(const Hypertable *ht, const Hypercube *cube)
{
	List *chunk_data_nodes = NIL;
	const Dimension *space_dim = hyperspace_get_closed_dimension(ht->space, 0);

	if (NULL != space_dim && NULL != space_dim->dimension_partitions)
	{
		const DimensionSlice *slice =
			ts_hypercube_get_slice_by_dimension_id(cube, space_dim->fd.id);
		const DimensionPartition *dp =
			ts_dimension_partition_find(space_dim->dimension_partitions, slice->fd.range_start);
		ListCell *lc;

		/* Filter out data nodes that aren't available */
		foreach (lc, dp->data_nodes)
		{
			char *node_name = lfirst(lc);

			if (ts_data_node_is_available(node_name))
				chunk_data_nodes = lappend(chunk_data_nodes, node_name);
		}
	}
	else
	{
		List *available_nodes = ts_hypertable_get_available_data_nodes(ht, false);
		int num_assigned = MIN(ht->fd.replication_factor, list_length(available_nodes));
		int n, i;

		n = hypertable_get_chunk_round_robin_index(ht, cube);

		for (i = 0; i < num_assigned; i++)
		{
			int j = (n + i) % list_length(available_nodes);
			HypertableDataNode *hdn = list_nth(available_nodes, j);
			chunk_data_nodes = lappend(chunk_data_nodes, NameStr(hdn->fd.node_name));
		}
	}

	if (chunk_data_nodes == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_TS_INSUFFICIENT_NUM_DATA_NODES),
				 (errmsg("insufficient number of data nodes"),
				  errhint("Increase the number of available data nodes on hypertable \"%s\".",
						  get_rel_name(ht->main_table_relid)))));

	if (list_length(chunk_data_nodes) < ht->fd.replication_factor)
		ereport(WARNING,
				(errcode(ERRCODE_TS_INSUFFICIENT_NUM_DATA_NODES),
				 errmsg("insufficient number of data nodes"),
				 errdetail("There are not enough data nodes to replicate chunks according to the"
						   " configured replication factor."),
				 errhint("Attach %d or more data nodes to hypertable \"%s\".",
						 ht->fd.replication_factor - list_length(chunk_data_nodes),
						 NameStr(ht->fd.table_name))));

#if defined(USE_ASSERT_CHECKING)
	assert_chunk_data_nodes_is_a_set(chunk_data_nodes);
#endif

	return chunk_data_nodes;
}

typedef bool (*hypertable_data_node_filter)(const HypertableDataNode *hdn);

static bool
filter_non_blocked_data_nodes(const HypertableDataNode *node)
{
	if (!ts_data_node_is_available(NameStr(node->fd.node_name)))
		return false;

	return !node->fd.block_chunks;
}

typedef void *(*get_value)(const HypertableDataNode *hdn);

static void *
get_hypertable_data_node_name(const HypertableDataNode *node)
{
	return pstrdup(NameStr(node->fd.node_name));
}

static void *
get_hypertable_data_node(const HypertableDataNode *node)
{
	HypertableDataNode *copy = palloc(sizeof(HypertableDataNode));

	memcpy(copy, node, sizeof(HypertableDataNode));
	return copy;
}

static List *
get_hypertable_data_node_values(const Hypertable *ht, hypertable_data_node_filter filter,
								get_value valuefunc)
{
	List *list = NULL;
	ListCell *cell;

	foreach (cell, ht->data_nodes)
	{
		HypertableDataNode *node = lfirst(cell);
		if (filter == NULL || filter(node))
			list = lappend(list, valuefunc(node));
	}

	return list;
}

List *
ts_hypertable_get_data_node_name_list(const Hypertable *ht)
{
	return get_hypertable_data_node_values(ht, NULL, get_hypertable_data_node_name);
}

static List *
get_available_data_nodes(const Hypertable *ht, get_value valuefunc, bool error_if_missing)
{
	List *available_nodes =
		get_hypertable_data_node_values(ht, filter_non_blocked_data_nodes, valuefunc);
	if (available_nodes == NIL && error_if_missing)
		ereport(ERROR,
				(errcode(ERRCODE_TS_INSUFFICIENT_NUM_DATA_NODES),
				 (errmsg("insufficient number of data nodes"),
				  errhint("Increase the number of available data nodes on hypertable \"%s\".",
						  get_rel_name(ht->main_table_relid)))));
	return available_nodes;
}

List *
ts_hypertable_get_available_data_nodes(const Hypertable *ht, bool error_if_missing)
{
	return get_available_data_nodes(ht, get_hypertable_data_node, error_if_missing);
}

List *
ts_hypertable_get_available_data_node_names(const Hypertable *ht, bool error_if_missing)
{
	return get_available_data_nodes(ht, get_hypertable_data_node_name, error_if_missing);
}

static List *
get_hypertable_data_node_ids(const Hypertable *ht, hypertable_data_node_filter filter)
{
	List *nodeids = NIL;
	ListCell *lc;

	foreach (lc, ht->data_nodes)
	{
		HypertableDataNode *node = lfirst(lc);
		if (filter == NULL || filter(node))
			nodeids = lappend_oid(nodeids, node->foreign_server_oid);
	}

	return nodeids;
}

List *
ts_hypertable_get_data_node_serverids_list(const Hypertable *ht)
{
	return get_hypertable_data_node_ids(ht, NULL);
}

List *
ts_hypertable_get_available_data_node_server_oids(const Hypertable *ht)
{
	return get_hypertable_data_node_ids(ht, filter_non_blocked_data_nodes);
}

HypertableType
ts_hypertable_get_type(const Hypertable *ht)
{
	Assert(ht->fd.replication_factor >= -1);
	if (ht->fd.replication_factor < 1)
		return (HypertableType) ht->fd.replication_factor;
	return HYPERTABLE_DISTRIBUTED;
}

void
ts_hypertable_func_call_on_data_nodes(const Hypertable *ht, FunctionCallInfo fcinfo)
{
	if (hypertable_is_distributed(ht))
		ts_cm_functions->func_call_on_data_nodes(fcinfo, ts_hypertable_get_data_node_name_list(ht));
}

/*
 * Get the max value of an open dimension.
 */
int64
ts_hypertable_get_open_dim_max_value(const Hypertable *ht, int dimension_index, bool *isnull)
{
	StringInfo command;
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
	command = makeStringInfo();
	appendStringInfo(command,
					 "SELECT pg_catalog.max(%s) FROM %s.%s",
					 quote_identifier(NameStr(dim->fd.column_name)),
					 quote_identifier(NameStr(ht->fd.schema_name)),
					 quote_identifier(NameStr(ht->fd.table_name)));

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "could not connect to SPI");

	res = SPI_execute(command->data, true /* read_only */, 0 /*count*/);

	if (res < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 (errmsg("could not find the maximum time value for hypertable \"%s\"",
						 get_rel_name(ht->main_table_relid)))));

	Ensure(SPI_gettypeid(SPI_tuptable->tupdesc, 1) == timetype,
		   "partition types for result (%d) and dimension (%d) do not match",
		   SPI_gettypeid(SPI_tuptable->tupdesc, 1),
		   ts_dimension_get_partition_type(dim));
	maxdat = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &max_isnull);

	if (isnull)
		*isnull = max_isnull;

	if ((res = SPI_finish()) != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed: %s", SPI_result_code_string(res));

	return max_isnull ? ts_time_get_min(timetype) : ts_time_value_to_internal(maxdat, timetype);
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

bool
ts_hypertable_update_dimension_partitions(const Hypertable *ht)
{
	const Dimension *space_dim = hyperspace_get_closed_dimension(ht->space, 0);

	if (NULL != space_dim)
	{
		List *data_node_names = NIL;

		if (hypertable_is_distributed(ht))
			data_node_names = ts_hypertable_get_available_data_node_names(ht, false);

		ts_dimension_partition_info_recreate(space_dim->fd.id,
											 space_dim->fd.num_slices,
											 data_node_names,
											 ht->fd.replication_factor);
		return true;
	}

	return false;
}
