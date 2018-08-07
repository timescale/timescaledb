#include <postgres.h>
#include <access/htup_details.h>
#include <access/heapam.h>
#include <access/relscan.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <utils/memutils.h>
#include <utils/builtins.h>
#include <utils/acl.h>
#include <utils/snapmgr.h>
#include <nodes/memnodes.h>
#include <nodes/makefuncs.h>
#include <nodes/value.h>
#include <catalog/namespace.h>
#include <catalog/pg_inherits_fn.h>
#include <catalog/pg_constraint_fn.h>
#include <catalog/pg_constraint.h>
#include <catalog/indexing.h>
#include <catalog/pg_proc.h>
#include <commands/tablespace.h>
#include <commands/dbcommands.h>
#include <commands/schemacmds.h>
#include <commands/tablecmds.h>
#include <commands/trigger.h>
#include <storage/lmgr.h>
#include <miscadmin.h>

#include "hypertable.h"
#include "dimension.h"
#include "chunk.h"
#include "chunk_adaptive.h"
#include "compat.h"
#include "subspace_store.h"
#include "hypertable_cache.h"
#include "trigger.h"
#include "scanner.h"
#include "catalog.h"
#include "dimension_slice.h"
#include "dimension_vector.h"
#include "hypercube.h"
#include "indexing.h"
#include "guc.h"
#include "errors.h"
#include "copy.h"
#include "utils.h"

Oid
rel_get_owner(Oid relid)
{
	HeapTuple	tuple;
	Oid			ownerid;

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
hypertable_has_privs_of(Oid hypertable_oid, Oid userid)
{
	return has_privs_of_role(userid, rel_get_owner(hypertable_oid));
}

Oid
hypertable_permissions_check(Oid hypertable_oid, Oid userid)
{
	Oid			ownerid = rel_get_owner(hypertable_oid);

	if (!has_privs_of_role(userid, ownerid))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for relation \"%s\"",
						get_rel_name(hypertable_oid))));

	return ownerid;
}

Hypertable *
hypertable_from_tuple(HeapTuple tuple, MemoryContext mctx)
{
	Hypertable *h;
	Oid			namespace_oid;

	h = MemoryContextAllocZero(mctx, sizeof(Hypertable));
	memcpy(&h->fd, GETSTRUCT(tuple), sizeof(FormData_hypertable));
	namespace_oid = get_namespace_oid(NameStr(h->fd.schema_name), false);
	h->main_table_relid = get_relname_relid(NameStr(h->fd.table_name), namespace_oid);
	h->space = dimension_scan(h->fd.id, h->main_table_relid, h->fd.num_dimensions, mctx);
	h->chunk_cache = subspace_store_init(h->space, mctx, guc_max_cached_chunks_per_hypertable);

	if (!heap_attisnull(tuple, Anum_hypertable_chunk_sizing_func_schema) &&
		!heap_attisnull(tuple, Anum_hypertable_chunk_sizing_func_name))
	{
		FuncCandidateList func =
		FuncnameGetCandidates(list_make2(makeString(NameStr(h->fd.chunk_sizing_func_schema)),
										 makeString(NameStr(h->fd.chunk_sizing_func_name))),
							  3, NIL, false, false, false);

		if (NULL == func || NULL != func->next)
			elog(ERROR, "could not find the adaptive chunking function \"%s.%s\"",
				 NameStr(h->fd.chunk_sizing_func_schema),
				 NameStr(h->fd.chunk_sizing_func_name));

		h->chunk_sizing_func = func->oid;
	}

	return h;
}

static bool
hypertable_tuple_get_relid(TupleInfo *ti, void *data)
{
	FormData_hypertable *form = (FormData_hypertable *) GETSTRUCT(ti->tuple);
	Oid		   *relid = data;
	Oid			schema_oid = get_namespace_oid(NameStr(form->schema_name), true);

	if (OidIsValid(schema_oid))
		*relid = get_relname_relid(NameStr(form->table_name), schema_oid);

	return false;
}

Oid
hypertable_id_to_relid(int32 hypertable_id)
{
	Catalog    *catalog = catalog_get();
	Oid			relid = InvalidOid;
	ScanKeyData scankey[1];
	ScannerCtx	scanctx = {
		.table = catalog->tables[HYPERTABLE].id,
		.index = catalog->tables[HYPERTABLE].index_ids[HYPERTABLE_ID_INDEX],
		.nkeys = 1,
		.scankey = scankey,
		.tuple_found = hypertable_tuple_get_relid,
		.data = &relid,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	/* Perform an index scan on the hypertable pkey. */
	ScanKeyInit(&scankey[0], Anum_hypertable_pkey_idx_id,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(hypertable_id));

	scanner_scan(&scanctx);

	return relid;
}

typedef struct ChunkStoreEntry
{
	MemoryContext mcxt;
	Chunk	   *chunk;
} ChunkStoreEntry;

static void
chunk_store_entry_free(void *cse)
{
	MemoryContextDelete(((ChunkStoreEntry *) cse)->mcxt);
}

static int
hypertable_scan_limit_internal(ScanKeyData *scankey,
							   int num_scankeys,
							   int indexid,
							   tuple_found_func on_tuple_found,
							   void *scandata,
							   int limit,
							   LOCKMODE lock,
							   bool tuplock,
							   MemoryContext mctx)
{
	Catalog    *catalog = catalog_get();
	ScannerCtx	scanctx = {
		.table = catalog->tables[HYPERTABLE].id,
		.index = CATALOG_INDEX(catalog, HYPERTABLE, indexid),
		.nkeys = num_scankeys,
		.scankey = scankey,
		.data = scandata,
		.limit = limit,
		.tuple_found = on_tuple_found,
		.lockmode = lock,
		.scandirection = ForwardScanDirection,
		.result_mctx = mctx,
		.tuplock = {
			.waitpolicy = LockWaitBlock,
			.lockmode = LockTupleExclusive,
			.enabled = tuplock,
		}
	};

	return scanner_scan(&scanctx);
}


static bool
hypertable_tuple_update(TupleInfo *ti, void *data)
{
	Hypertable *ht = data;
	Datum		values[Natts_hypertable];
	bool		nulls[Natts_hypertable];
	HeapTuple	copy;
	CatalogSecurityContext sec_ctx;

	heap_deform_tuple(ti->tuple, ti->desc, values, nulls);

	values[Anum_hypertable_schema_name - 1] = NameGetDatum(&ht->fd.schema_name);
	values[Anum_hypertable_table_name - 1] = NameGetDatum(&ht->fd.table_name);
	values[Anum_hypertable_associated_schema_name - 1] = NameGetDatum(&ht->fd.associated_schema_name);
	values[Anum_hypertable_associated_table_prefix - 1] = NameGetDatum(&ht->fd.associated_table_prefix);
	values[Anum_hypertable_num_dimensions - 1] = Int16GetDatum(ht->fd.num_dimensions);
	values[Anum_hypertable_chunk_target_size - 1] = Int64GetDatum(ht->fd.chunk_target_size);

	memset(nulls, 0, sizeof(nulls));

	if (OidIsValid(ht->chunk_sizing_func))
	{
		Dimension  *dim = hyperspace_get_dimension(ht->space, DIMENSION_TYPE_OPEN, 0);
		ChunkSizingInfo info = {
			.table_relid = ht->main_table_relid,
			.colname = dim == NULL ? NULL : NameStr(dim->fd.column_name),
			.func = ht->chunk_sizing_func,
		};

		chunk_adaptive_sizing_info_validate(&info);

		namestrcpy(&ht->fd.chunk_sizing_func_schema, NameStr(info.func_schema));
		namestrcpy(&ht->fd.chunk_sizing_func_name, NameStr(info.func_name));

		values[Anum_hypertable_chunk_sizing_func_schema - 1] =
			NameGetDatum(&ht->fd.chunk_sizing_func_schema);
		values[Anum_hypertable_chunk_sizing_func_name - 1] =
			NameGetDatum(&ht->fd.chunk_sizing_func_name);
	}
	else
	{
		nulls[Anum_hypertable_chunk_sizing_func_schema - 1] = true;
		nulls[Anum_hypertable_chunk_sizing_func_name - 1] = true;
	}

	copy = heap_form_tuple(ti->desc, values, nulls);

	catalog_become_owner(catalog_get(), &sec_ctx);
	catalog_update_tid(ti->scanrel, &ti->tuple->t_self, copy);
	catalog_restore_user(&sec_ctx);

	heap_freetuple(copy);

	return false;
}

int
hypertable_update(Hypertable *ht)
{
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0], Anum_hypertable_pkey_idx_id,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(ht->fd.id));

	return hypertable_scan_limit_internal(scankey,
										  1,
										  HYPERTABLE_ID_INDEX,
										  hypertable_tuple_update,
										  ht,
										  1,
										  RowExclusiveLock,
										  false,
										  CurrentMemoryContext);
}

int
hypertable_scan_with_memory_context(const char *schema,
									const char *table,
									tuple_found_func tuple_found,
									void *data,
									LOCKMODE lockmode,
									bool tuplock,
									MemoryContext mctx)
{
	ScanKeyData scankey[2];
	NameData	schema_name,
				table_name;

	namestrcpy(&schema_name, schema);
	namestrcpy(&table_name, table);

	/* Perform an index scan on schema and table. */
	ScanKeyInit(&scankey[0], Anum_hypertable_name_idx_schema,
				BTEqualStrategyNumber, F_NAMEEQ,
				NameGetDatum(&schema_name));
	ScanKeyInit(&scankey[1], Anum_hypertable_name_idx_table,
				BTEqualStrategyNumber, F_NAMEEQ,
				NameGetDatum(&table_name));

	return hypertable_scan_limit_internal(scankey, 2,
										  HYPERTABLE_NAME_INDEX,
										  tuple_found,
										  data,
										  1,
										  lockmode,
										  tuplock,
										  mctx);
}

int
hypertable_scan_relid(Oid table_relid,
					  tuple_found_func tuple_found,
					  void *data,
					  LOCKMODE lockmode,
					  bool tuplock)
{
	return hypertable_scan(get_namespace_name(get_rel_namespace(table_relid)),
						   get_rel_name(table_relid),
						   tuple_found,
						   data,
						   lockmode,
						   tuplock);
}

static bool
hypertable_tuple_delete(TupleInfo *ti, void *data)
{
	CatalogSecurityContext sec_ctx;
	bool		isnull;
	int			hypertable_id = heap_getattr(ti->tuple, Anum_hypertable_id, ti->desc, &isnull);

	tablespace_delete(hypertable_id, NULL);
	chunk_delete_by_hypertable_id(hypertable_id);
	dimension_delete_by_hypertable_id(hypertable_id, true);

	catalog_become_owner(catalog_get(), &sec_ctx);
	catalog_delete(ti->scanrel, ti->tuple);
	catalog_restore_user(&sec_ctx);

	return true;
}

int
hypertable_delete_by_id(int32 hypertable_id)
{
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0], Anum_hypertable_pkey_idx_id,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(hypertable_id));

	return hypertable_scan_limit_internal(scankey,
										  1,
										  HYPERTABLE_ID_INDEX,
										  hypertable_tuple_delete,
										  NULL,
										  1,
										  RowExclusiveLock,
										  false,
										  CurrentMemoryContext);
}


int
hypertable_delete_by_name(const char *schema_name, const char *table_name)
{
	ScanKeyData scankey[2];

	ScanKeyInit(&scankey[0], Anum_hypertable_name_idx_schema,
				BTEqualStrategyNumber, F_NAMEEQ,
				DirectFunctionCall1(namein, CStringGetDatum(schema_name)));

	ScanKeyInit(&scankey[1], Anum_hypertable_name_idx_table,
				BTEqualStrategyNumber, F_NAMEEQ,
				DirectFunctionCall1(namein, CStringGetDatum(table_name)));

	return hypertable_scan_limit_internal(scankey,
										  2,
										  HYPERTABLE_NAME_INDEX,
										  hypertable_tuple_delete,
										  NULL,
										  0,
										  RowExclusiveLock,
										  false,
										  CurrentMemoryContext);
}

static bool
reset_associated_tuple_found(TupleInfo *ti, void *data)
{
	HeapTuple	tuple = heap_copytuple(ti->tuple);
	FormData_hypertable *form = (FormData_hypertable *) GETSTRUCT(tuple);
	CatalogSecurityContext sec_ctx;

	namestrcpy(&form->associated_schema_name, INTERNAL_SCHEMA_NAME);
	catalog_become_owner(catalog_get(), &sec_ctx);
	catalog_update(ti->scanrel, tuple);
	catalog_restore_user(&sec_ctx);

	heap_freetuple(tuple);

	return true;
}

/*
 * Reset the matching associated schema to the internal schema.
 */
int
hypertable_reset_associated_schema_name(const char *associated_schema)
{
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0], Anum_hypertable_associated_schema_name,
				BTEqualStrategyNumber, F_NAMEEQ,
				DirectFunctionCall1(namein, CStringGetDatum(associated_schema)));

	return hypertable_scan_limit_internal(scankey,
										  1,
										  INVALID_INDEXID,
										  reset_associated_tuple_found,
										  NULL,
										  0,
										  RowExclusiveLock,
										  false,
										  CurrentMemoryContext);
}

static bool
tuple_found_lock(TupleInfo *ti, void *data)
{
	HTSU_Result *result = data;

	*result = ti->lockresult;
	return false;
}

HTSU_Result
hypertable_lock_tuple(Oid table_relid)
{
	HTSU_Result result;
	int			num_found;

	num_found = hypertable_scan(get_namespace_name(get_rel_namespace(table_relid)),
								get_rel_name(table_relid),
								tuple_found_lock,
								&result,
								RowExclusiveLock,
								true);

	if (num_found != 1)
		ereport(ERROR,
				(errcode(ERRCODE_IO_HYPERTABLE_NOT_EXIST),
				 errmsg("table \"%s\" is not a hypertable",
						get_rel_name(table_relid))));

	return result;
}

bool
hypertable_lock_tuple_simple(Oid table_relid)
{
	HTSU_Result result = hypertable_lock_tuple(table_relid);

	switch (result)
	{
		case HeapTupleSelfUpdated:

			/*
			 * Updated by the current transaction already. We equate this with
			 * a successul lock since the tuple should be locked if updated by
			 * us.
			 */
			return true;
		case HeapTupleMayBeUpdated:
			/* successfully locked */
			return true;
		case HeapTupleUpdated:
			ereport(ERROR,
					(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
					 errmsg("hypertable \"%s\" has already been updated by another transaction",
							get_rel_name(table_relid)),
					 errhint("Retry the operation again")));
		case HeapTupleBeingUpdated:
			ereport(ERROR,
					(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
					 errmsg("hypertable \"%s\" is being updated by another transaction",
							get_rel_name(table_relid)),
					 errhint("Retry the operation again")));
		case HeapTupleWouldBlock:
			/* Locking would block. Let caller decide what to do */
			return false;
		case HeapTupleInvisible:
			elog(ERROR, "attempted to lock invisible tuple");
			return false;
		default:
			elog(ERROR, "unexpected tuple lock status");
			return false;
	}
}

int
hypertable_set_name(Hypertable *ht, const char *newname)
{
	namestrcpy(&ht->fd.table_name, newname);

	return hypertable_update(ht);
}

int
hypertable_set_schema(Hypertable *ht, const char *newname)
{
	namestrcpy(&ht->fd.schema_name, newname);

	return hypertable_update(ht);
}

int
hypertable_set_num_dimensions(Hypertable *ht, int16 num_dimensions)
{
	Assert(num_dimensions > 0);
	ht->fd.num_dimensions = num_dimensions;
	return hypertable_update(ht);
}

#define DEFAULT_ASSOCIATED_TABLE_PREFIX_FORMAT "_hyper_%d"

static void
hypertable_insert_relation(Relation rel,
						   Name schema_name,
						   Name table_name,
						   Name associated_schema_name,
						   Name associated_table_prefix,
						   Name chunk_sizing_func_schema,
						   Name chunk_sizing_func_name,
						   int64 chunk_target_size,
						   int16 num_dimensions)
{
	TupleDesc	desc = RelationGetDescr(rel);
	Datum		values[Natts_hypertable];
	bool		nulls[Natts_hypertable] = {false};
	NameData	default_associated_table_prefix;
	CatalogSecurityContext sec_ctx;

	values[Anum_hypertable_schema_name - 1] = NameGetDatum(schema_name);
	values[Anum_hypertable_table_name - 1] = NameGetDatum(table_name);
	values[Anum_hypertable_associated_schema_name - 1] = NameGetDatum(associated_schema_name);
	values[Anum_hypertable_num_dimensions - 1] = Int16GetDatum(num_dimensions);

	if (NULL != chunk_sizing_func_schema && NULL != chunk_sizing_func_name)
	{
		values[Anum_hypertable_chunk_sizing_func_schema - 1] = NameGetDatum(chunk_sizing_func_schema);
		values[Anum_hypertable_chunk_sizing_func_name - 1] = NameGetDatum(chunk_sizing_func_name);
	}
	else
	{
		nulls[Anum_hypertable_chunk_sizing_func_schema - 1] = true;
		nulls[Anum_hypertable_chunk_sizing_func_name - 1] = true;
	}

	if (chunk_target_size < 0)
		chunk_target_size = 0;

	values[Anum_hypertable_chunk_target_size - 1] = Int64GetDatum(chunk_target_size);

	catalog_become_owner(catalog_get(), &sec_ctx);
	values[Anum_hypertable_id - 1] = Int32GetDatum(catalog_table_next_seq_id(catalog_get(), HYPERTABLE));

	if (NULL != associated_table_prefix)
		values[Anum_hypertable_associated_table_prefix - 1] = NameGetDatum(associated_table_prefix);
	else
	{
		memset(NameStr(default_associated_table_prefix), '\0', NAMEDATALEN);
		snprintf(NameStr(default_associated_table_prefix),
				 NAMEDATALEN,
				 DEFAULT_ASSOCIATED_TABLE_PREFIX_FORMAT,
				 DatumGetInt32(values[Anum_hypertable_id - 1]));
		values[Anum_hypertable_associated_table_prefix - 1] =
			NameGetDatum(&default_associated_table_prefix);
	}

	catalog_insert_values(rel, desc, values, nulls);
	catalog_restore_user(&sec_ctx);
}

static void
hypertable_insert(Name schema_name,
				  Name table_name,
				  Name associated_schema_name,
				  Name associated_table_prefix,
				  Name chunk_sizing_func_schema,
				  Name chunk_sizing_func_name,
				  int64 chunk_target_size,
				  int16 num_dimensions)
{
	Catalog    *catalog = catalog_get();
	Relation	rel;

	rel = heap_open(catalog->tables[HYPERTABLE].id, RowExclusiveLock);
	hypertable_insert_relation(rel,
							   schema_name,
							   table_name,
							   associated_schema_name,
							   associated_table_prefix,
							   chunk_sizing_func_schema,
							   chunk_sizing_func_name,
							   chunk_target_size,
							   num_dimensions);
	heap_close(rel, RowExclusiveLock);
}

static bool
hypertable_tuple_found(TupleInfo *ti, void *data)
{
	Hypertable **entry = data;

	*entry = hypertable_from_tuple(ti->tuple, ti->mctx);
	return false;
}

Hypertable *
hypertable_get_by_name(char *schema, char *name)
{
	Hypertable *ht = NULL;

	hypertable_scan(schema,
					name,
					hypertable_tuple_found,
					&ht,
					AccessShareLock,
					false);

	return ht;
}

Hypertable *
hypertable_get_by_id(int32 hypertable_id)
{
	ScanKeyData scankey[1];
	Hypertable *ht = NULL;

	ScanKeyInit(&scankey[0], Anum_hypertable_pkey_idx_id,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(hypertable_id));

	hypertable_scan_limit_internal(scankey,
								   1,
								   HYPERTABLE_ID_INDEX,
								   hypertable_tuple_found,
								   &ht,
								   1,
								   AccessShareLock,
								   false,
								   CurrentMemoryContext);
	return ht;
}

static ChunkStoreEntry *
hypertable_chunk_store_add(Hypertable *h, Chunk *chunk)
{
	ChunkStoreEntry *cse;
	MemoryContext old_mcxt,
				chunk_mcxt;

	chunk_mcxt = AllocSetContextCreate(subspace_store_mcxt(h->chunk_cache),
									   "chunk cache entry memory context",
									   ALLOCSET_SMALL_SIZES);

	/* Add the chunk to the subspace store */
	old_mcxt = MemoryContextSwitchTo(chunk_mcxt);
	cse = palloc(sizeof(ChunkStoreEntry));
	cse->mcxt = chunk_mcxt;
	cse->chunk = chunk_copy(chunk);
	subspace_store_add(h->chunk_cache, chunk->cube, cse, chunk_store_entry_free);
	MemoryContextSwitchTo(old_mcxt);

	return cse;
}

Chunk *
hypertable_get_chunk(Hypertable *h, Point *point)
{
	ChunkStoreEntry *cse = subspace_store_get(h->chunk_cache, point);

	if (NULL == cse)
	{
		Chunk	   *chunk;

		/*
		 * chunk_find() must execute on a per-tuple memory context since it
		 * allocates a lot of transient data. We don't want this allocated on
		 * the cache's memory context.
		 */
		chunk = chunk_find(h->space, point);

		if (NULL == chunk)
			chunk = chunk_create(h, point,
								 NameStr(h->fd.associated_schema_name),
								 NameStr(h->fd.associated_table_prefix));

		Assert(chunk != NULL);

		/* Also add the chunk to the hypertable's chunk store */
		cse = hypertable_chunk_store_add(h, chunk);
	}

	Assert(NULL != cse);
	Assert(NULL != cse->chunk);
	Assert(MemoryContextContains(cse->mcxt, cse));

	return cse->chunk;
}

bool
hypertable_has_tablespace(Hypertable *ht, Oid tspc_oid)
{
	Tablespaces *tspcs = tablespace_scan(ht->fd.id);

	return tablespaces_contain(tspcs, tspc_oid);
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
hypertable_select_tablespace(Hypertable *ht, Chunk *chunk)
{
	Dimension  *dim;
	DimensionVec *vec;
	DimensionSlice *slice;
	Tablespaces *tspcs = tablespace_scan(ht->fd.id);
	int			i = 0;

	if (NULL == tspcs || tspcs->num_tablespaces == 0)
		return NULL;

	dim = hyperspace_get_closed_dimension(ht->space, 0);

	if (NULL == dim)
		dim = hyperspace_get_open_dimension(ht->space, 0);

	Assert(NULL != dim && (IS_OPEN_DIMENSION(dim) || dim->fd.num_slices > 0));

	vec = dimension_get_slices(dim);

	Assert(NULL != vec && (IS_OPEN_DIMENSION(dim) || vec->num_slices > 0));

	slice = hypercube_get_slice_by_dimension_id(chunk->cube, dim->fd.id);

	Assert(NULL != slice);

	/*
	 * Find the index (ordinal) of the chunk's slice in the dimension we
	 * picked
	 */
	i = dimension_vec_find_slice_index(vec, slice->fd.id);

	Assert(i >= 0);

	/* Use the index of the slice to find the tablespace */
	return &tspcs->tablespaces[i % tspcs->num_tablespaces];
}

char *
hypertable_select_tablespace_name(Hypertable *ht, Chunk *chunk)
{
	Tablespace *tspc = hypertable_select_tablespace(ht, chunk);

	if (NULL == tspc)
		return NULL;

	return NameStr(tspc->fd.tablespace_name);
}

/*
 * Get the tablespace at an offset from the given tablespace.
 */
Tablespace *
hypertable_get_tablespace_at_offset_from(Hypertable *ht, Oid tablespace_oid, int16 offset)
{
	Tablespaces *tspcs = tablespace_scan(ht->fd.id);
	int			i = 0;

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
	Cache	   *hcache = hypertable_cache_pin();
	Hypertable *ht = hypertable_cache_get_entry(hcache, relid);
	Oid			result = (ht == NULL) ? InvalidOid : ht->main_table_relid;

	cache_release(hcache);

	return result;
}

/*
 * Returns a hypertable's relation ID (OID) iff the given RangeVar corresponds to
 * a hypertable, otherwise InvalidOid.
*/
Oid
hypertable_relid(RangeVar *rv)
{
	return hypertable_relid_lookup(RangeVarGetRelid(rv, NoLock, true));
}

bool
is_hypertable(Oid relid)
{
	if (!OidIsValid(relid))
		return false;
	return hypertable_relid_lookup(relid) != InvalidOid;
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
	Oid			schema_oid;

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
		if (pg_database_aclcheck(MyDatabaseId, user_oid, ACL_CREATE) != ACLCHECK_OK)
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("permissions denied: cannot create schema \"%s\" in database \"%s\"",
							schema_name,
							get_database_name(MyDatabaseId))));
	}
	else if (pg_namespace_aclcheck(schema_oid, user_oid, ACL_CREATE) != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permissions denied: cannot create chunks in schema \"%s\"",
						schema_name)));

	return schema_oid;
}

static bool
relation_has_tuples(Relation rel)
{
	HeapScanDesc scandesc = heap_beginscan(rel, GetActiveSnapshot(), 0, NULL);
	bool		hastuples = HeapTupleIsValid(heap_getnext(scandesc, ForwardScanDirection));

	heap_endscan(scandesc);
	return hastuples;
}

static bool
table_has_tuples(Oid table_relid, LOCKMODE lockmode)
{
	Relation	rel = heap_open(table_relid, lockmode);
	bool		hastuples = relation_has_tuples(rel);

	heap_close(rel, lockmode);
	return hastuples;
}

static bool
table_is_logged(Oid table_relid)
{
	return get_rel_persistence(table_relid) == RELPERSISTENCE_PERMANENT;
}

static bool
table_has_replica_identity(Relation rel)
{
	return rel->rd_rel->relreplident != REPLICA_IDENTITY_DEFAULT;
}

static bool inline
table_has_rules(Relation rel)
{
	return rel->rd_rules != NULL;
}


bool
hypertable_has_tuples(Oid table_relid, LOCKMODE lockmode)
{
	ListCell   *lc;
	List	   *chunks = find_inheritance_children(table_relid, lockmode);

	foreach(lc, chunks)
	{
		Oid			chunk_relid = lfirst_oid(lc);

		/* Chunks already locked by find_inheritance_children() */
		if (table_has_tuples(chunk_relid, NoLock))
			return true;
	}

	return false;
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

	CreateSchemaCommand(&stmt,
						"(generated CREATE SCHEMA command)"
#if PG10
						,-1, -1
#endif
		);
}

/*
 * Check that existing table constraints are supported.
 *
 * Hypertables do not support some constraints. For instance, NO INHERIT
 * constraints cannot be enforced on a hypertable since they only exist on the
 * parent table, which will have no tuples.
 */
static void
hypertable_validate_constraints(Oid relid)
{
	Relation	catalog;
	SysScanDesc scan;
	ScanKeyData scankey;
	HeapTuple	tuple;

	catalog = heap_open(ConstraintRelationId, AccessShareLock);

	ScanKeyInit(&scankey, Anum_pg_constraint_conrelid, BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(relid));

	scan = systable_beginscan(catalog, ConstraintRelidIndexId, true,
							  NULL, 1, &scankey);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_constraint form = (Form_pg_constraint) GETSTRUCT(tuple);

		if (form->contype == CONSTRAINT_CHECK && form->connoinherit)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("cannot have NO INHERIT constraints on hypertable \"%s\"",
							get_rel_name(relid)),
					 errhint("Remove all NO INHERIT constraints from table \"%s\" before making it a hypertable.",
							 get_rel_name(relid))));
	}

	systable_endscan(scan);
	heap_close(catalog, AccessShareLock);
}

/*
 * Functionality to block INSERTs on the hypertable's root table.
 *
 * The design considered implementing this either with RULES, constraints, or
 * triggers. An "internal" trigger was found to have the best trade-offs:
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
 * - A trigger, especially an "internal" one, is transparent (doesn't show up
 *	 on \d+ <table>) and is automatically removed when the extension is
 *	 dropped (since it is part of the extension). Internal triggers aren't
 *	 inherited by chunks either, so we need no special handling to _not_
 *	 inherit the blocking trigger.
 */
#define INSERT_BLOCKER_NAME "insert_blocker"

TS_FUNCTION_INFO_V1(hypertable_insert_blocker);

Datum
hypertable_insert_blocker(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	const char *relname = get_rel_name(trigdata->tg_relation->rd_id);

	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "insert_blocker: not called by trigger manager");

	if (guc_restoring)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot INSERT into hypertable \"%s\" during restore", relname),
				 errhint("Set 'timescaledb.restoring' to 'OFF' after the restore process has finished.")));
	else
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("invalid INSERT on the root table of hypertable \"%s\"", relname),
				 errhint("Make sure the TimescaleDB extension has been preloaded.")));

	PG_RETURN_NULL();
}

/*
 * Get the insert blocker trigger on a table.
 *
 * Note that we cannot get the insert trigger by name since internal triggers
 * are made unique by appending the trigger OID, which we do not
 * know. Instead, we have to search all triggers.
 */
static Oid
insert_blocker_trigger_get(Oid relid)
{
	Relation	tgrel;
	ScanKeyData skey[1];
	SysScanDesc tgscan;
	HeapTuple	tuple;
	Oid			tgoid = InvalidOid;

	tgrel = heap_open(TriggerRelationId, AccessShareLock);

	ScanKeyInit(&skey[0],
				Anum_pg_trigger_tgrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	tgscan = systable_beginscan(tgrel, TriggerRelidNameIndexId, true,
								NULL, 1, skey);

	while (HeapTupleIsValid(tuple = systable_getnext(tgscan)))
	{
		Form_pg_trigger trig = (Form_pg_trigger) GETSTRUCT(tuple);

		if (TRIGGER_TYPE_MATCHES(trig->tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_INSERT) &&
			strncmp(INSERT_BLOCKER_NAME, NameStr(trig->tgname), strlen(INSERT_BLOCKER_NAME)) == 0 &&
			trig->tgisinternal)
		{
			tgoid = HeapTupleGetOid(tuple);
			break;
		}
	}

	systable_endscan(tgscan);
	heap_close(tgrel, AccessShareLock);

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
	char	   *relname = get_rel_name(relid);
	Oid			schemaid = get_rel_namespace(relid);
	char	   *schema = get_namespace_name(schemaid);
	CreateTrigStmt stmt = {
		.type = T_CreateTrigStmt,
		.row = true,
		.timing = TRIGGER_TYPE_BEFORE,
		.trigname = INSERT_BLOCKER_NAME,	/* Note, internal triggers get the
											 * OID appended to the name */
		.relation = makeRangeVar(schema, relname, -1),
		.funcname = list_make2(makeString(INTERNAL_SCHEMA_NAME), makeString(INSERT_BLOCKER_NAME)),
		.args = NIL,
		.events = TRIGGER_TYPE_INSERT,
	};

	objaddr.objectId = insert_blocker_trigger_get(relid);

	/* Trigger already exists. Do nothing */
	if (OidIsValid(objaddr.objectId))
		return objaddr.objectId;

	/*
	 * Create as an internal trigger; it won't show up with \d and won't be
	 * inherited by chunks.
	 */
	objaddr = CreateTrigger(&stmt, NULL, relid, InvalidOid, InvalidOid, InvalidOid, true);

	if (!OidIsValid(objaddr.objectId))
		elog(ERROR, "could not create insert blocker trigger");

	return objaddr.objectId;
}

TS_FUNCTION_INFO_V1(hypertable_insert_blocker_trigger_add);

/*
 * This function is exposed to add the blocking trigger on legacy hypertables
 * that don't have the trigger. We can't do it from SQL code, because internal
 * triggers cannot be added from SQL.
 *
 * In case the hypertable's root table has data in it, we bail out with an
 * error instructing the user to fix the issue first.
 */
Datum
hypertable_insert_blocker_trigger_add(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);

	if (table_has_tuples(relid, AccessShareLock))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("hypertable \"%s\" has data in the root table", get_rel_name(relid)),
				 errdetail("Migrate the data from the root table to chunks before running the UPDATE again."),
				 errhint("Data can be migrated as follows:\n"
						 "> BEGIN;\n"
						 "> SET timescaledb.restoring = 'OFF';\n"
						 "> INSERT INTO \"%1$s\" SELECT * FROM ONLY \"%1$s\";\n"
						 "> SET timescaledb.restoring = 'ON';\n"
						 "> TRUNCATE ONLY \"%1$s\";\n"
						 "> SET timescaledb.restoring = 'OFF';\n"
						 "> COMMIT;", get_rel_name(relid))));

	PG_RETURN_OID(insert_blocker_trigger_add(relid));
}

TS_FUNCTION_INFO_V1(hypertable_create);

/*
 * Create a hypertable from an existing table.
 *
 * Arguments:
 * main_table              REGCLASS
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
 * chunk_sizing_func       OID = NULL
 * chunk_target_size       TEXT = NULL
 */
Datum
hypertable_create(PG_FUNCTION_ARGS)
{
	Oid			table_relid = PG_GETARG_OID(0);
	Name		associated_schema_name = PG_ARGISNULL(4) ? NULL : PG_GETARG_NAME(4);
	Name		associated_table_prefix = PG_ARGISNULL(5) ? NULL : PG_GETARG_NAME(5);
	bool		create_default_indexes = PG_ARGISNULL(7) ? false : PG_GETARG_BOOL(7);
	bool		if_not_exists = PG_ARGISNULL(8) ? false : PG_GETARG_BOOL(8);
	bool		migrate_data = PG_ARGISNULL(10) ? false : PG_GETARG_BOOL(10);
	DimensionInfo time_dim_info = {
		.table_relid = table_relid,
		.colname = PG_ARGISNULL(1) ? NULL : PG_GETARG_NAME(1),
		.interval_datum = PG_ARGISNULL(6) ? Int64GetDatum(-1) : PG_GETARG_DATUM(6),
		.interval_type = PG_ARGISNULL(6) ? InvalidOid : get_fn_expr_argtype(fcinfo->flinfo, 6),
	};
	DimensionInfo space_dim_info = {
		.table_relid = table_relid,
		.colname = PG_ARGISNULL(2) ? NULL : PG_GETARG_NAME(2),
		.num_slices = PG_ARGISNULL(3) ? -1 : PG_GETARG_INT16(3),
		.num_slices_is_set = !PG_ARGISNULL(3),
		.partitioning_func = PG_ARGISNULL(9) ? InvalidOid : PG_GETARG_OID(9),
	};
	ChunkSizingInfo chunk_sizing_info = {
		.table_relid = table_relid,
		.target_size = PG_ARGISNULL(11) ? NULL : PG_GETARG_TEXT_P(11),
		.func = PG_ARGISNULL(12) ? InvalidOid : PG_GETARG_OID(12),
		.colname = PG_ARGISNULL(1) ? NULL : PG_GETARG_CSTRING(1),
		.check_for_index = !create_default_indexes,
	};
	Cache	   *hcache;
	Hypertable *ht;
	Oid			associated_schema_oid;
	Oid			user_oid = GetUserId();
	Oid			tspc_oid = get_rel_tablespace(table_relid);
	bool		table_has_data;
	NameData	schema_name,
				table_name,
				default_associated_schema_name;
	Relation	rel;

	/* quick exit in the easy if-not-exists case to avoid all locking */
	if (if_not_exists && is_hypertable(table_relid))
	{
		ereport(NOTICE,
				(errcode(ERRCODE_IO_HYPERTABLE_EXISTS),
				 errmsg("table \"%s\" is already a hypertable, skipping",
						get_rel_name(table_relid))));

		PG_RETURN_VOID();
	}

	/*
	 * Serialize hypertable creation to avoid having multiple transactions
	 * creating the same hypertable simultaneously. The lock should conflict
	 * with itself and RowExclusive, to prevent simultaneous inserts on the
	 * table. Also since TRUNCATE (part of data migrationt) takes an
	 * AccessExclusiveLock take that lock level here to so that we don't have
	 * lock upgrades, which are suceptible to deadlocks. If we aren't
	 * migrating data, then shouldn't have much contention on the table thus
	 * not worth optimizing.
	 */
	rel = heap_open(table_relid, AccessExclusiveLock);

	/* recheck after getting lock */
	if (is_hypertable(table_relid))
	{
		/*
		 * Unlock and return. Note that unlocking is analagous to what PG does
		 * for ALTER TABLE ADD COLUMN IF NOT EXIST
		 */
		heap_close(rel, AccessExclusiveLock);

		if (if_not_exists)
		{
			ereport(NOTICE,
					(errcode(ERRCODE_IO_HYPERTABLE_EXISTS),
					 errmsg("table \"%s\" is already a hypertable, skipping",
							get_rel_name(table_relid))));

			PG_RETURN_VOID();
		}

		ereport(ERROR,
				(errcode(ERRCODE_IO_HYPERTABLE_EXISTS),
				 errmsg("table \"%s\" is already a hypertable",
						get_rel_name(table_relid))));
	}

	/*
	 * Check that the user has permissions to make this table into a
	 * hypertable
	 */
	hypertable_permissions_check(table_relid, user_oid);

	/* Is this the right kind of relation? */
	switch (get_rel_relkind(table_relid))
	{
#if PG10
		case RELKIND_PARTITIONED_TABLE:
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("table \"%s\" is already partitioned", get_rel_name(table_relid)),
					 errdetail("It is not possible to turn partitioned tables into hypertables.")));
#endif
		case RELKIND_RELATION:
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("invalid relation type")));
	}

	/* Check that the table doesn't have any unsupported constraints */
	hypertable_validate_constraints(table_relid);

	table_has_data = relation_has_tuples(rel);

	if (!migrate_data && table_has_data)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("table \"%s\" is not empty", get_rel_name(table_relid)),
				 errhint("You can migrate data by specifying 'migrate_data => true' when calling this function.")));

	if (is_inheritance_table(table_relid))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("table \"%s\" is already partitioned", get_rel_name(table_relid)),
				 errdetail("It is not possible to turn tables that use inheritance into hypertables.")));

	if (!table_is_logged(table_relid))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("table \"%s\" has to be logged", get_rel_name(table_relid)),
				 errdetail("It is not possible to turn temporary or unlogged tables into hypertables.")));

	if (table_has_replica_identity(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("table \"%s\" has replica identity set", get_rel_name(table_relid)),
				 errdetail("Logical replication is not supported on hypertables.")));

	if (table_has_rules(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("hypertables do not support rules"),
				 errdetail("Table \"%s\" has attached rules, which do not work on hypertables.", get_rel_name(table_relid)),
				 errhint("Remove the rules before calling create_hypertable")));

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
	if (relation_has_transition_table_trigger(table_relid))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("hypertables do not support transition tables in triggers")));

	/* Validate and set chunk sizing information */
	if (OidIsValid(chunk_sizing_info.func))
	{
		chunk_adaptive_sizing_info_validate(&chunk_sizing_info);

		if (chunk_sizing_info.target_size_bytes > 0)
			time_dim_info.adaptive_chunking = true;
	}

	/* Validate that the dimensions are OK */
	dimension_validate_info(&time_dim_info);

	if (DIMENSION_INFO_IS_SET(&space_dim_info))
		dimension_validate_info(&space_dim_info);

	/* Checks pass, now we can create the catalog information */
	namestrcpy(&schema_name, get_namespace_name(get_rel_namespace(table_relid)));
	namestrcpy(&table_name, get_rel_name(table_relid));

	hypertable_insert(&schema_name,
					  &table_name,
					  associated_schema_name,
					  associated_table_prefix,
					  &chunk_sizing_info.func_schema,
					  &chunk_sizing_info.func_name,
					  chunk_sizing_info.target_size_bytes,
					  DIMENSION_INFO_IS_SET(&space_dim_info) ? 2 : 1);

	/* Get the a Hypertable object via the cache */
	hcache = hypertable_cache_pin();
	time_dim_info.ht = space_dim_info.ht = hypertable_cache_get_entry(hcache, table_relid);

	Assert(time_dim_info.ht != NULL);

	/* Add validated dimensions */
	dimension_add_from_info(&time_dim_info);

	if (DIMENSION_INFO_IS_SET(&space_dim_info))
		dimension_add_from_info(&space_dim_info);

	/* Refresh the cache to get the updated hypertable with added dimensions */
	cache_release(hcache);
	hcache = hypertable_cache_pin();
	ht = hypertable_cache_get_entry(hcache, table_relid);

	Assert(ht != NULL);

	/* Verify that existing indexes are compatible with a hypertable */
	indexing_verify_indexes(ht);

	/* Attach tablespace, if any */
	if (OidIsValid(tspc_oid))
	{
		NameData	tspc_name;

		namestrcpy(&tspc_name, get_tablespace_name(tspc_oid));
		tablespace_attach_internal(&tspc_name, table_relid, false);
	}

	/*
	 * Migrate data from the main table to chunks
	 *
	 * Note: we do not unlock here. We wait till the end of the txn instead.
	 * Must close the relation before migrating data.
	 */
	heap_close(rel, NoLock);

	if (table_has_data)
	{
		ereport(NOTICE,
				(errmsg("migrating data to chunks"),
				 errdetail("Migration might take a while depending on the amount of data.")));

		timescaledb_move_from_table_to_chunks(ht, AccessShareLock);
	}

	insert_blocker_trigger_add(table_relid);

	if (create_default_indexes)
		indexing_create_default_indexes(ht);

	cache_release(hcache);
	PG_RETURN_VOID();
}
