#include <postgres.h>
#include <fmgr.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <utils/spccache.h>
#include <utils/acl.h>
#include <utils/builtins.h>
#include <utils/fmgroids.h>
#include <commands/tablespace.h>
#include <miscadmin.h>

#include "hypertable_cache.h"
#include "errors.h"
#include "catalog.h"
#include "scanner.h"
#include "tablespace.h"
#include "compat.h"

#define TABLESPACE_DEFAULT_CAPACITY 4

static Oid
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

static Tablespaces *
tablespaces_alloc(int capacity)
{
	Tablespaces *tspcs;

	tspcs = palloc(sizeof(Tablespaces));
	tspcs->capacity = capacity;
	tspcs->num_tablespaces = 0;
	tspcs->tablespaces = palloc(sizeof(Tablespace) * tspcs->capacity);

	return tspcs;
}

Tablespace *
tablespaces_add(Tablespaces *tspcs, int32 tspc_id, Oid tspc_oid)
{
	Tablespace *tspc;

	if (tspcs->num_tablespaces >= tspcs->capacity)
	{
		tspcs->capacity += TABLESPACE_DEFAULT_CAPACITY;
		tspcs->tablespaces = repalloc(tspcs->tablespaces, sizeof(Tablespace) * tspcs->capacity);
	}

	tspc = &tspcs->tablespaces[tspcs->num_tablespaces++];
	tspc->tablespace_id = tspc_id;
	tspc->tablespace_oid = tspc_oid;

	return tspc;
}

static bool
tablespace_tuple_found(TupleInfo *ti, void *data)
{
	Tablespaces *tspcs = data;
	FormData_tablespace *form = (FormData_tablespace *) GETSTRUCT(ti->tuple);

	tablespaces_add(tspcs,
					form->id,
					get_tablespace_oid(NameStr(form->tablespace_name), true));
	return true;
}

Tablespaces *
tablespace_scan(int32 hypertable_id)
{
	Catalog    *catalog = catalog_get();
	Tablespaces *tspcs = tablespaces_alloc(TABLESPACE_DEFAULT_CAPACITY);
	ScanKeyData scankey[1];
	ScannerCtx	scanctx = {
		.table = catalog->tables[TABLESPACE].id,
		.index = catalog->tables[TABLESPACE].index_ids[TABLESPACE_HYPERTABLE_ID_TABLESPACE_NAME_IDX],
		.scantype = ScannerTypeIndex,
		.nkeys = 1,
		.scankey = scankey,
		.data = tspcs,
		.tuple_found = tablespace_tuple_found,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	ScanKeyInit(&scankey[0], Anum_tablespace_hypertable_id_tablespace_name_idx_hypertable_id,
			  BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(hypertable_id));

	scanner_scan(&scanctx);

	return tspcs;
}

static int32
tablespace_insert_relation(Relation rel, int32 hypertable_id, const char *tspcname)
{
	TupleDesc	desc = RelationGetDescr(rel);
	Datum		values[Natts_tablespace];
	bool		nulls[Natts_tablespace] = {false};
	int32		id;

	memset(values, 0, sizeof(values));
	id = catalog_table_next_seq_id(catalog_get(), TABLESPACE);
	values[Anum_tablespace_id - 1] = Int32GetDatum(id);
	values[Anum_tablespace_hypertable_id - 1] = Int32GetDatum(hypertable_id);
	values[Anum_tablespace_tablespace_name - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(tspcname));

	catalog_insert_values(rel, desc, values, nulls);

	return id;
}

static int32
tablespace_insert(int32 hypertable_id, const char *tspcname)
{
	Catalog    *catalog = catalog_get();
	Relation	rel;
	int32		id;

	rel = heap_open(catalog->tables[TABLESPACE].id, RowExclusiveLock);
	id = tablespace_insert_relation(rel, hypertable_id, tspcname);
	heap_close(rel, RowExclusiveLock);

	return id;
}

TS_FUNCTION_INFO_V1(tablespace_attach);

Datum
tablespace_attach(PG_FUNCTION_ARGS)
{
	Oid			hypertable_oid;
	Name		tspcname;
	Cache	   *hcache;
	Hypertable *ht;
	Oid			tspc_oid;
	int32		tspc_id;
	Oid			user_oid = GetUserId();
	Oid			ownerid;
	AclResult	aclresult;
	MemoryContext old;
	CatalogSecurityContext sec_ctx;

	if (PG_NARGS() != 2)
		elog(ERROR, "Invalid number of arguments");

	if (PG_ARGISNULL(0))
		elog(ERROR, "Invalid tablespace name");

	if (PG_ARGISNULL(1))
		elog(ERROR, "Invalid hypertable");

	tspcname = PG_GETARG_NAME(0);
	hypertable_oid = PG_GETARG_OID(1);

	tspc_oid = get_tablespace_oid(NameStr(*tspcname), true);

	if (!OidIsValid(tspc_oid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
			  errmsg("Tablespace \"%s\" does not exist", NameStr(*tspcname)),
				 errhint("A tablespace needs to be created"
						 " before attaching it to a hypertable")));

	ownerid = rel_get_owner(hypertable_oid);

	if (!has_privs_of_role(user_oid, ownerid))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("User \"%s\" lacks permissions on table \"%s\"",
		  GetUserNameFromId(user_oid, true), get_rel_name(hypertable_oid))));

	aclresult = pg_tablespace_aclcheck(tspc_oid, ownerid, ACL_CREATE);

	if (aclresult != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
		 errmsg("Table owner \"%s\" lacks permissions for tablespace \"%s\"",
				GetUserNameFromId(ownerid, true), NameStr(*tspcname))));

	hcache = hypertable_cache_pin();
	ht = hypertable_cache_get_entry(hcache, hypertable_oid);

	if (NULL == ht)
		ereport(ERROR,
				(errcode(ERRCODE_IO_HYPERTABLE_NOT_EXIST),
				 errmsg("Table \"%s\" is not a hypertable", get_rel_name(hypertable_oid))));

	if (hypertable_has_tablespace(ht, tspc_oid))
		ereport(ERROR,
				(errcode(ERRCODE_IO_TABLESPACE_ALREADY_ATTACHED),
		 errmsg("Tablespace \"%s\" is already attached to hypertable \"%s\"",
				NameStr(*tspcname), get_rel_name(hypertable_oid))));

	catalog_become_owner(catalog_get(), &sec_ctx);
	tspc_id = tablespace_insert(ht->fd.id, NameStr(*tspcname));
	catalog_restore_user(&sec_ctx);

	/* Add the tablespace to the hypertable */
	old = cache_switch_to_memory_context(hcache);
	hypertable_add_tablespace(ht, tspc_id, tspc_oid);
	MemoryContextSwitchTo(old);

	cache_release(hcache);

	PG_RETURN_VOID();
}
