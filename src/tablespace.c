#include <postgres.h>
#include <fmgr.h>
#include <utils/lsyscache.h>
#include <utils/spccache.h>
#include <utils/acl.h>
#include <utils/builtins.h>
#include <utils/fmgroids.h>
#include <commands/tablespace.h>
#include <access/xact.h>
#include <miscadmin.h>

#include "hypertable_cache.h"
#include "errors.h"
#include "catalog.h"
#include "scanner.h"
#include "tablespace.h"
#include "compat.h"

#define TABLESPACE_DEFAULT_CAPACITY 4

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
tablespaces_add(Tablespaces *tspcs, FormData_tablespace *form, Oid tspc_oid)
{
	Tablespace *tspc;

	if (tspcs->num_tablespaces >= tspcs->capacity)
	{
		tspcs->capacity += TABLESPACE_DEFAULT_CAPACITY;
		tspcs->tablespaces = repalloc(tspcs->tablespaces, sizeof(Tablespace) * tspcs->capacity);
	}

	tspc = &tspcs->tablespaces[tspcs->num_tablespaces++];
	memcpy(&tspc->fd, form, sizeof(FormData_tablespace));
	tspc->tablespace_oid = tspc_oid;

	return tspc;
}

int
tablespaces_clear(Tablespaces *tspcs)
{
	int			num = tspcs->num_tablespaces;

	tspcs->num_tablespaces = 0;

	return num;
}

bool
tablespaces_delete(Tablespaces *tspcs, Oid tspc_oid)
{
	int			i;

	for (i = 0; i < tspcs->num_tablespaces; i++)
	{
		if (tspc_oid == tspcs->tablespaces[i].tablespace_oid)
		{
			memcpy(&tspcs->tablespaces[i],
				   &tspcs->tablespaces[i + 1],
				   sizeof(Tablespace) * (tspcs->num_tablespaces - i - 1));
			tspcs->num_tablespaces--;
			return true;
		}
	}

	return false;
}

static bool
tablespace_tuple_found(TupleInfo *ti, void *data)
{
	Tablespaces *tspcs = data;
	FormData_tablespace *form = (FormData_tablespace *) GETSTRUCT(ti->tuple);
	Oid			tspcoid = get_tablespace_oid(NameStr(form->tablespace_name), true);

	tablespaces_add(tspcs, form, tspcoid);

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

typedef struct TablespaceScanInfo
{
	Catalog    *catalog;
	Cache	   *hcache;
	Oid			userid;
	int			num_filtered;
	int			stopcount;
} TablespaceScanInfo;

static bool
tablespace_tuple_delete(TupleInfo *ti, void *data)
{
	TablespaceScanInfo *info = data;
	CatalogSecurityContext sec_ctx;

	catalog_become_owner(info->catalog, &sec_ctx);
	catalog_delete_only(ti->scanrel, ti->tuple);
	catalog_restore_user(&sec_ctx);

	return (info->stopcount == 0 || ti->count < info->stopcount);
}

static int
tablespace_delete(int32 hypertable_id, const char *tspcname)
{
	ScanKeyData scankey[2];
	TablespaceScanInfo info = {
		.catalog = catalog_get(),
		.stopcount = (NULL != tspcname),
	};
	ScannerCtx	scanctx = {
		.table = info.catalog->tables[TABLESPACE].id,
		.index = info.catalog->tables[TABLESPACE].index_ids[TABLESPACE_HYPERTABLE_ID_TABLESPACE_NAME_IDX],
		.scantype = ScannerTypeIndex,
		.nkeys = 0,
		.scankey = scankey,
		.tuple_found = tablespace_tuple_delete,
		.data = &info,
		.lockmode = RowExclusiveLock,
		.scandirection = ForwardScanDirection,
	};
	int			num_deleted;

	ScanKeyInit(&scankey[scanctx.nkeys++],
			 Anum_tablespace_hypertable_id_tablespace_name_idx_hypertable_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(hypertable_id));

	if (NULL != tspcname)
		ScanKeyInit(&scankey[scanctx.nkeys++],
		   Anum_tablespace_hypertable_id_tablespace_name_idx_tablespace_name,
					BTEqualStrategyNumber,
					F_NAMEEQ,
					DirectFunctionCall1(namein, CStringGetDatum(tspcname)));

	num_deleted = scanner_scan(&scanctx);

	if (num_deleted > 0)
	{
		catalog_invalidate_cache(catalog_table_get_id(info.catalog, TABLESPACE), CMD_DELETE);
		CommandCounterIncrement();
	}

	return num_deleted;
}

static bool
tablespace_tuple_owner_filter(TupleInfo *ti, void *data)
{
	TablespaceScanInfo *info = data;
	FormData_tablespace *form = (FormData_tablespace *) GETSTRUCT(ti->tuple);
	Hypertable *ht;

	ht = hypertable_cache_get_entry_by_id(info->hcache, form->hypertable_id);

	Assert(NULL != ht);

	if (hypertable_has_privs_of(ht->main_table_relid, info->userid))
		return true;

	info->num_filtered++;

	return false;
}

static int
tablespace_delete_from_all(const char *tspcname, Oid userid)
{
	ScanKeyData scankey[1];
	TablespaceScanInfo info = {
		.catalog = catalog_get(),
		.hcache = hypertable_cache_pin(),
		.userid = userid,
	};
	ScannerCtx	scanctx = {
		.table = info.catalog->tables[TABLESPACE].id,
		.scantype = ScannerTypeHeap,
		.nkeys = 1,
		.scankey = scankey,
		.tuple_found = tablespace_tuple_delete,
		.filter = tablespace_tuple_owner_filter,
		.data = &info,
		.lockmode = RowExclusiveLock,
		.scandirection = ForwardScanDirection,
	};
	int			num_deleted;

	ScanKeyInit(&scankey[0],
				Anum_tablespace_tablespace_name,
				BTEqualStrategyNumber, F_NAMEEQ,
				DirectFunctionCall1(namein, CStringGetDatum(tspcname)));

	cache_release(info.hcache);

	num_deleted = scanner_scan(&scanctx);

	if (num_deleted > 0)
	{
		catalog_invalidate_cache(catalog_table_get_id(info.catalog, TABLESPACE), CMD_DELETE);
		CommandCounterIncrement();
	}

	if (info.num_filtered > 0)
		elog(NOTICE, "Tablespace \"%s\" remains attached to %d hypertable(s) due to lack of permissions",
			 tspcname, info.num_filtered);

	return num_deleted;
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

	ownerid = hypertable_permissions_check(hypertable_oid, GetUserId());

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
				 errmsg("Table \"%s\" is not a hypertable",
						get_rel_name(hypertable_oid))));

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

static int
tablespace_detach_one(Oid hypertable_oid, const char *tspcname, Oid tspcoid)
{
	Cache	   *hcache;
	Hypertable *ht;
	int			ret;

	hypertable_permissions_check(hypertable_oid, GetUserId());

	hcache = hypertable_cache_pin();
	ht = hypertable_cache_get_entry(hcache, hypertable_oid);

	if (NULL == ht)
		ereport(ERROR,
				(errcode(ERRCODE_IO_HYPERTABLE_NOT_EXIST),
				 errmsg("Table \"%s\" is not a hypertable",
						get_rel_name(hypertable_oid))));

	if (!hypertable_has_tablespace(ht, tspcoid))
		ereport(ERROR,
				(errcode(ERRCODE_IO_TABLESPACE_NOT_ATTACHED),
			 errmsg("Tablespace \"%s\" is not attached to hypertable \"%s\"",
					tspcname, get_rel_name(hypertable_oid))));

	ret = tablespace_delete(ht->fd.id, tspcname);
	hypertable_delete_tablespace(ht, tspcoid);

	cache_release(hcache);

	return ret;
}

static int
tablespace_detach_all(Oid hypertable_oid)
{
	Cache	   *hcache;
	Hypertable *ht;
	int			ret;

	hypertable_permissions_check(hypertable_oid, GetUserId());

	hcache = hypertable_cache_pin();
	ht = hypertable_cache_get_entry(hcache, hypertable_oid);

	if (NULL == ht)
		ereport(ERROR,
				(errcode(ERRCODE_IO_HYPERTABLE_NOT_EXIST),
				 errmsg("Table \"%s\" is not a hypertable",
						get_rel_name(hypertable_oid))));

	ret = tablespace_delete(ht->fd.id, NULL);

	cache_release(hcache);

	return ret;
}

TS_FUNCTION_INFO_V1(tablespace_detach);

Datum
tablespace_detach(PG_FUNCTION_ARGS)
{
	Oid			hypertable_oid = InvalidOid;
	Name		tspcname;
	Oid			tspcoid;
	int			ret;

	switch (PG_NARGS())
	{
		case 1:
			tspcname = PG_GETARG_NAME(0);
			break;
		case 2:
			tspcname = PG_GETARG_NAME(0);
			hypertable_oid = PG_ARGISNULL(1) ? InvalidOid : PG_GETARG_OID(1);
			break;
		default:
			elog(ERROR, "Invalid number of arguments");
	}

	if (NULL == tspcname)
		elog(ERROR, "Invalid tablespace name");

	tspcoid = get_tablespace_oid(NameStr(*tspcname), true);

	if (!OidIsValid(tspcoid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("No tablespace \"%s\" exists.",
						NameStr(*tspcname))));

	if (OidIsValid(hypertable_oid))
		ret = tablespace_detach_one(hypertable_oid, NameStr(*tspcname), tspcoid);
	else
		ret = tablespace_delete_from_all(NameStr(*tspcname), GetUserId());

	PG_RETURN_INT32(ret);
}

TS_FUNCTION_INFO_V1(tablespace_detach_all_from_hypertable);

Datum
tablespace_detach_all_from_hypertable(PG_FUNCTION_ARGS)
{
	if (PG_NARGS() != 1)
		elog(ERROR, "Invalid number of arguments");

	if (PG_ARGISNULL(0))
		elog(ERROR, "Invalid argument");

	PG_RETURN_INT32(tablespace_detach_all(PG_GETARG_OID(0)));
}
