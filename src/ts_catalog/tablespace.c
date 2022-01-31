/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <fmgr.h>
#include <utils/lsyscache.h>
#include <utils/spccache.h>
#include <utils/acl.h>
#include <utils/builtins.h>
#include <utils/fmgroids.h>
#include <commands/tablespace.h>
#include <commands/tablecmds.h>
#include <access/xact.h>
#include <miscadmin.h>
#include <funcapi.h>

#include "hypertable_cache.h"
#include "errors.h"
#include "ts_catalog/catalog.h"
#include "scanner.h"
#include "ts_catalog/tablespace.h"
#include "compat/compat.h"

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
ts_tablespaces_add(Tablespaces *tspcs, const FormData_tablespace *form, Oid tspc_oid)
{
	Tablespace *tspc;

	if (tspcs->num_tablespaces >= tspcs->capacity)
	{
		tspcs->capacity += TABLESPACE_DEFAULT_CAPACITY;
		Assert(tspcs->tablespaces); /* repalloc() does not work with NULL argument */
		tspcs->tablespaces = repalloc(tspcs->tablespaces, sizeof(Tablespace) * tspcs->capacity);
	}

	tspc = &tspcs->tablespaces[tspcs->num_tablespaces++];
	memcpy(&tspc->fd, form, sizeof(FormData_tablespace));
	tspc->tablespace_oid = tspc_oid;

	return tspc;
}

bool
ts_tablespaces_contain(const Tablespaces *tspcs, Oid tspc_oid)
{
	int i;

	for (i = 0; i < tspcs->num_tablespaces; i++)
		if (tspc_oid == tspcs->tablespaces[i].tablespace_oid)
			return true;

	return false;
}

static ScanTupleResult
tablespace_tuple_found(TupleInfo *ti, void *data)
{
	Tablespaces *tspcs = data;
	bool should_free;
	HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
	FormData_tablespace *form = (FormData_tablespace *) GETSTRUCT(tuple);
	Oid tspcoid = get_tablespace_oid(NameStr(form->tablespace_name), true);

	if (NULL != tspcs)
		ts_tablespaces_add(tspcs, form, tspcoid);

	if (should_free)
		heap_freetuple(tuple);

	return SCAN_CONTINUE;
}

static int
tablespace_scan_internal(int indexid, ScanKeyData *scankey, int nkeys, tuple_found_func tuple_found,
						 tuple_filter_func tuple_filter, void *data, int limit, LOCKMODE lockmode)
{
	Catalog *catalog = ts_catalog_get();
	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, TABLESPACE),
		.index = catalog_get_index(catalog, TABLESPACE, indexid),
		.nkeys = nkeys,
		.scankey = scankey,
		.tuple_found = tuple_found,
		.filter = tuple_filter,
		.data = data,
		.limit = limit,
		.lockmode = lockmode,
		.scandirection = ForwardScanDirection,
	};

	return ts_scanner_scan(&scanctx);
}

Tablespaces *
ts_tablespace_scan(int32 hypertable_id)
{
	Tablespaces *tspcs = tablespaces_alloc(TABLESPACE_DEFAULT_CAPACITY);
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_tablespace_hypertable_id_tablespace_name_idx_hypertable_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(hypertable_id));

	tablespace_scan_internal(TABLESPACE_HYPERTABLE_ID_TABLESPACE_NAME_IDX,
							 scankey,
							 1,
							 tablespace_tuple_found,
							 NULL,
							 tspcs,
							 0,
							 AccessShareLock);

	return tspcs;
}

typedef struct TablespaceScanInfo
{
	CatalogDatabaseInfo *database_info;
	Cache *hcache;
	Oid userid;
	int num_filtered;
	int stopcount;
	List *hypertables; /* Hypertables affected, where applicable */
	void *data;
} TablespaceScanInfo;

static int
tablespace_scan_by_name(const char *tspcname, tuple_found_func tuple_found, void *data)
{
	ScanKeyData scankey[1];
	int nkeys = 0;

	if (NULL != tspcname)
		ScanKeyInit(&scankey[nkeys++],
					Anum_tablespace_tablespace_name,
					BTEqualStrategyNumber,
					F_NAMEEQ,
					CStringGetDatum(tspcname));

	return tablespace_scan_internal(INVALID_INDEXID,
									scankey,
									nkeys,
									tuple_found,
									NULL,
									data,
									0,
									AccessShareLock);
}

int
ts_tablespace_count_attached(const char *tspcname)
{
	return tablespace_scan_by_name(tspcname, NULL, NULL);
}

static void
tablespace_validate_revoke_internal(const char *tspcname, tuple_found_func tuple_found, void *stmt)
{
	TablespaceScanInfo info = {
		.database_info = ts_catalog_database_info_get(),
		.hcache = ts_hypertable_cache_pin(),
		.data = stmt,
	};

	tablespace_scan_by_name(tspcname, tuple_found, &info);

	ts_cache_release(info.hcache);
}

static void
validate_revoke_create(Oid tspcoid, Oid role, Oid relid)
{
	AclResult aclresult = pg_tablespace_aclcheck(tspcoid, role, ACL_CREATE);

	if (aclresult != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
				 errmsg("cannot revoke privilege while tablespace \"%s\" is attached to hypertable "
						"\"%s\"",
						get_tablespace_name(tspcoid),
						get_rel_name(relid)),
				 errhint("Detach the tablespace before revoking the privilege on it.")));
}

/*
 * Verify that the REVOKE of permissions on a tablespace does not make it
 * impossible to use the tablespace for new chunks.
 *
 * This check should be done after the REVOKE has been applied.
 */
static ScanTupleResult
revoke_tuple_found(TupleInfo *ti, void *data)
{
	TablespaceScanInfo *info = data;
	GrantStmt *stmt = info->data;
	ListCell *lc_role;
	bool isnull;
	Datum hyper_id;
	Datum tablespace_name;
	Oid tspcoid;
	Hypertable *ht;
	Oid relowner;

	hyper_id = slot_getattr(ti->slot, Anum_tablespace_hypertable_id, &isnull);
	Assert(!isnull);
	tablespace_name = slot_getattr(ti->slot, Anum_tablespace_tablespace_name, &isnull);
	Assert(!isnull);
	tspcoid = get_tablespace_oid(NameStr(*DatumGetName(tablespace_name)), false);
	ht = ts_hypertable_cache_get_entry_by_id(info->hcache, DatumGetInt32(hyper_id));
	relowner = ts_rel_get_owner(ht->main_table_relid);

	foreach (lc_role, stmt->grantees)
	{
		RoleSpec *role = lfirst(lc_role);
		Oid roleoid = get_role_oid_or_public(role->rolename);

		/* Check if this is a role we're interested in */
		if (!OidIsValid(roleoid))
			continue;

		/*
		 * A revoke on a tablespace can only be for 'CREATE' (or ALL), so no
		 * need to check which privilege is revoked.
		 */
		validate_revoke_create(tspcoid, relowner, ht->main_table_relid);
	}

	return SCAN_CONTINUE;
}

void
ts_tablespace_validate_revoke(GrantStmt *stmt)
{
	tablespace_validate_revoke_internal(strVal(linitial(stmt->objects)), revoke_tuple_found, stmt);
}

/*
 * Verify that the REVOKE of a role on a tablespace does not make it impossible
 * to use the tablespace for new chunks.
 *
 * This check should be done after the REVOKE has been applied.
 */
static ScanTupleResult
revoke_role_tuple_found(TupleInfo *ti, void *data)
{
	TablespaceScanInfo *info = data;
	GrantRoleStmt *stmt = info->data;
	bool isnull;
	Datum hyper_id;
	Datum tablespace_name;
	Oid tspcoid;
	Hypertable *ht;
	Oid relowner;
	ListCell *lc_role;

	hyper_id = slot_getattr(ti->slot, Anum_tablespace_hypertable_id, &isnull);
	Assert(!isnull);
	tablespace_name = slot_getattr(ti->slot, Anum_tablespace_tablespace_name, &isnull);
	Assert(!isnull);
	tspcoid = get_tablespace_oid(NameStr(*DatumGetName(tablespace_name)), false);
	ht = ts_hypertable_cache_get_entry_by_id(info->hcache, DatumGetInt32(hyper_id));
	relowner = ts_rel_get_owner(ht->main_table_relid);

	foreach (lc_role, stmt->grantee_roles)
	{
		RoleSpec *rolespec = lfirst(lc_role);
		Oid grantee = get_rolespec_oid(rolespec, true);

		/* Only interested in revokes on table owners */
		if (grantee != relowner)
			continue;

		/*
		 * No need to check which role that was revoked since we are only
		 * interested in the resulting permissions for the table owner. A
		 * table owner could have CREATE on the tablespace from multiple
		 * roles, so revoking one of those roles might not mean the owner no
		 * longer has CREATE on the tablespace.
		 */
		validate_revoke_create(tspcoid, relowner, ht->main_table_relid);
	}

	return SCAN_CONTINUE;
}

void
ts_tablespace_validate_revoke_role(GrantRoleStmt *stmt)
{
	tablespace_validate_revoke_internal(NULL, revoke_role_tuple_found, stmt);
}

static int32
tablespace_insert_relation(Relation rel, int32 hypertable_id, const char *tspcname)
{
	TupleDesc desc = RelationGetDescr(rel);
	Datum values[Natts_tablespace];
	bool nulls[Natts_tablespace] = { false };
	int32 id;

	memset(values, 0, sizeof(values));
	id = ts_catalog_table_next_seq_id(ts_catalog_get(), TABLESPACE);
	values[AttrNumberGetAttrOffset(Anum_tablespace_id)] = Int32GetDatum(id);
	values[AttrNumberGetAttrOffset(Anum_tablespace_hypertable_id)] = Int32GetDatum(hypertable_id);
	values[AttrNumberGetAttrOffset(Anum_tablespace_tablespace_name)] =
		DirectFunctionCall1(namein, CStringGetDatum(tspcname));

	ts_catalog_insert_values(rel, desc, values, nulls);

	return id;
}

static int32
tablespace_insert(int32 hypertable_id, const char *tspcname)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel;
	int32 id;

	rel = table_open(catalog_get_table_id(catalog, TABLESPACE), RowExclusiveLock);
	id = tablespace_insert_relation(rel, hypertable_id, tspcname);
	table_close(rel, RowExclusiveLock);

	return id;
}

static ScanTupleResult
tablespace_tuple_delete(TupleInfo *ti, void *data)
{
	TablespaceScanInfo *info = data;
	bool should_free;
	CatalogSecurityContext sec_ctx;
	HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
	FormData_tablespace *form = (FormData_tablespace *) GETSTRUCT(tuple);

	ts_catalog_database_info_become_owner(info->database_info, &sec_ctx);
	ts_catalog_delete_tid_only(ti->scanrel, ts_scanner_get_tuple_tid(ti));
	ts_catalog_restore_user(&sec_ctx);
	info->hypertables = lappend_int(info->hypertables, form->hypertable_id);

	if (should_free)
		heap_freetuple(tuple);

	return (info->stopcount == 0 || ti->count < info->stopcount) ? SCAN_CONTINUE : SCAN_DONE;
}

int
ts_tablespace_delete(int32 hypertable_id, const char *tspcname, Oid tspcoid)

{
	ScanKeyData scankey[2];
	TablespaceScanInfo info = {
		.database_info = ts_catalog_database_info_get(),
		.stopcount = (NULL != tspcname),
	};
	int num_deleted, nkeys = 0;

	ScanKeyInit(&scankey[nkeys++],
				Anum_tablespace_hypertable_id_tablespace_name_idx_hypertable_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(hypertable_id));

	if (NULL != tspcname)
		ScanKeyInit(&scankey[nkeys++],
					Anum_tablespace_hypertable_id_tablespace_name_idx_tablespace_name,
					BTEqualStrategyNumber,
					F_NAMEEQ,
					CStringGetDatum(tspcname));

	num_deleted = tablespace_scan_internal(TABLESPACE_HYPERTABLE_ID_TABLESPACE_NAME_IDX,
										   scankey,
										   nkeys,
										   tablespace_tuple_delete,
										   NULL,
										   &info,
										   0,
										   RowExclusiveLock);

	if (num_deleted > 0)
		CommandCounterIncrement();

	return num_deleted;
}

static ScanFilterResult
tablespace_tuple_owner_filter(const TupleInfo *ti, void *data)
{
	TablespaceScanInfo *info = data;
	bool isnull;
	Datum hyper_id;
	Hypertable *ht;
	ScanFilterResult result;

	hyper_id = slot_getattr(ti->slot, Anum_tablespace_hypertable_id, &isnull);
	Assert(!isnull);
	ht = ts_hypertable_cache_get_entry_by_id(info->hcache, DatumGetInt32(hyper_id));
	Assert(NULL != ht);

	if (ts_hypertable_has_privs_of(ht->main_table_relid, info->userid))
		result = SCAN_INCLUDE;
	else
	{
		result = SCAN_EXCLUDE;
		info->num_filtered++;
	}

	return result;
}

/*
 * Detach a tablespace from all hypertables it is attached to.
 *
 * Output parameters:
 *   - `hypertables`: the list of hypertables that the tablespace was removed from.
 *
 * Returns:
 *   integer giving the number of tablespaces deleted.
 */
static int
tablespace_delete_from_all(const char *tspcname, Oid userid, List **hypertables)
{
	ScanKeyData scankey[1];
	TablespaceScanInfo info = {
		.database_info = ts_catalog_database_info_get(),
		.hcache = ts_hypertable_cache_pin(),
		.userid = userid,
	};
	int num_deleted;

	ScanKeyInit(&scankey[0],
				Anum_tablespace_tablespace_name,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				CStringGetDatum(tspcname));

	num_deleted = tablespace_scan_internal(INVALID_INDEXID,
										   scankey,
										   1,
										   tablespace_tuple_delete,
										   tablespace_tuple_owner_filter,
										   &info,
										   0,
										   RowExclusiveLock);
	ts_cache_release(info.hcache);

	if (num_deleted > 0)
		CommandCounterIncrement();

	if (info.num_filtered > 0)
		ereport(NOTICE,
				(errmsg("tablespace \"%s\" remains attached to %d hypertable(s) due to lack of "
						"permissions",
						tspcname,
						info.num_filtered)));

	*hypertables = info.hypertables;

	return num_deleted;
}

TS_FUNCTION_INFO_V1(ts_tablespace_attach);

Datum
ts_tablespace_attach(PG_FUNCTION_ARGS)
{
	Name tspcname = PG_ARGISNULL(0) ? NULL : PG_GETARG_NAME(0);
	Oid hypertable_oid = PG_ARGISNULL(1) ? InvalidOid : PG_GETARG_OID(1);
	bool if_not_attached = PG_ARGISNULL(2) ? false : PG_GETARG_BOOL(2);
	Relation rel;

	TS_PREVENT_FUNC_IF_READ_ONLY();

	if (PG_NARGS() < 2 || PG_NARGS() > 3)
		elog(ERROR, "invalid number of arguments");

	ts_tablespace_attach_internal(tspcname, hypertable_oid, if_not_attached);

	/* If the hypertable did not have a tablespace assigned, we set one */
	rel = relation_open(hypertable_oid, AccessShareLock);
	if (!OidIsValid(rel->rd_rel->reltablespace))
	{
		AlterTableCmd *const cmd = makeNode(AlterTableCmd);

		cmd->subtype = AT_SetTableSpace;
		cmd->name = NameStr(*tspcname);

		AlterTableInternal(hypertable_oid, list_make1(cmd), false);
	}
	relation_close(rel, AccessShareLock);
	PG_RETURN_VOID();
}

void
ts_tablespace_attach_internal(Name tspcname, Oid hypertable_oid, bool if_not_attached)
{
	Cache *hcache;
	Hypertable *ht;
	Oid tspc_oid;
	Oid ownerid;
	AclResult aclresult;
	CatalogSecurityContext sec_ctx;

	if (NULL == tspcname)
		elog(ERROR, "invalid tablespace name");

	if (!OidIsValid(hypertable_oid))
		elog(ERROR, "invalid hypertable");

	tspc_oid = get_tablespace_oid(NameStr(*tspcname), true);

	if (!OidIsValid(tspc_oid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("tablespace \"%s\" does not exist", NameStr(*tspcname)),
				 errhint("The tablespace needs to be created"
						 " before attaching it to a hypertable.")));

	ownerid = ts_hypertable_permissions_check(hypertable_oid, GetUserId());

	/*
	 * Only check permissions on tablespace if it is not the database default.
	 * In usual case users can create tables in their database which will use
	 * the default tablespace of the database. This condition makes sure they
	 * can also always move a table from another tablespace to the default of
	 * their own database. Related to this issue in postgres core:
	 * https://www.postgresql.org/message-id/52DC8AEA.7090507%402ndquadrant.com
	 * Which was handled in a similar way. (See
	 * tablecmds.c::ATPrepSetTableSpace)
	 */
	if (tspc_oid != MyDatabaseTableSpace)
	{
		/*
		 * Note that we check against the table owner rather than the current
		 * user here, since we're not actually creating a table using this
		 * tablespace at this point
		 */
		aclresult = pg_tablespace_aclcheck(tspc_oid, ownerid, ACL_CREATE);

		if (aclresult != ACLCHECK_OK)
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("permission denied for tablespace \"%s\" by table owner \"%s\"",
							NameStr(*tspcname),
							GetUserNameFromId(ownerid, true))));
	}
	ht = ts_hypertable_cache_get_cache_and_entry(hypertable_oid, CACHE_FLAG_NONE, &hcache);

	if (hypertable_is_distributed(ht))
		ereport(ERROR,
				(errcode(ERRCODE_TS_OPERATION_NOT_SUPPORTED),
				 errmsg("cannot attach tablespace to distributed hypertable")));

	if (ts_hypertable_has_tablespace(ht, tspc_oid))
	{
		if (if_not_attached)
			ereport(NOTICE,
					(errcode(ERRCODE_TS_TABLESPACE_ALREADY_ATTACHED),
					 errmsg("tablespace \"%s\" is already attached to hypertable \"%s\", skipping",
							NameStr(*tspcname),
							get_rel_name(hypertable_oid))));
		else
			ereport(ERROR,
					(errcode(ERRCODE_TS_TABLESPACE_ALREADY_ATTACHED),
					 errmsg("tablespace \"%s\" is already attached to hypertable \"%s\"",
							NameStr(*tspcname),
							get_rel_name(hypertable_oid))));
	}
	else
	{
		ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
		tablespace_insert(ht->fd.id, NameStr(*tspcname));
		ts_catalog_restore_user(&sec_ctx);
	}

	ts_cache_release(hcache);
}

static int
tablespace_detach_one(Oid hypertable_oid, const char *tspcname, Oid tspcoid, bool if_attached)
{
	Cache *hcache;
	Hypertable *ht;
	int ret = 0;

	ts_hypertable_permissions_check(hypertable_oid, GetUserId());

	ht = ts_hypertable_cache_get_cache_and_entry(hypertable_oid, CACHE_FLAG_NONE, &hcache);

	if (ts_hypertable_has_tablespace(ht, tspcoid))
		ret = ts_tablespace_delete(ht->fd.id, tspcname, tspcoid);
	else if (if_attached)
		ereport(NOTICE,
				(errcode(ERRCODE_TS_TABLESPACE_NOT_ATTACHED),
				 errmsg("tablespace \"%s\" is not attached to hypertable \"%s\", skipping",
						tspcname,
						get_rel_name(hypertable_oid))));
	else
		ereport(ERROR,
				(errcode(ERRCODE_TS_TABLESPACE_NOT_ATTACHED),
				 errmsg("tablespace \"%s\" is not attached to hypertable \"%s\"",
						tspcname,
						get_rel_name(hypertable_oid))));

	ts_cache_release(hcache);

	return ret;
}

static int
tablespace_detach_all(Oid hypertable_oid)
{
	Cache *hcache;
	Hypertable *ht;
	int ret;

	ts_hypertable_permissions_check(hypertable_oid, GetUserId());

	ht = ts_hypertable_cache_get_cache_and_entry(hypertable_oid, CACHE_FLAG_NONE, &hcache);

	ret = ts_tablespace_delete(ht->fd.id, NULL, InvalidOid);

	ts_cache_release(hcache);

	return ret;
}

static void
detach_tablespace_from_hypertable_if_set(Oid hypertable_oid, Oid tspcoid)
{
	Relation rel;

	Assert(OidIsValid(hypertable_oid) && OidIsValid(tspcoid));

	rel = relation_open(hypertable_oid, AccessShareLock);
	if (OidIsValid(rel->rd_rel->reltablespace) && rel->rd_rel->reltablespace == tspcoid)
	{
		AlterTableCmd *const cmd = makeNode(AlterTableCmd);

		cmd->subtype = AT_SetTableSpace;
		cmd->name = "pg_default";

		AlterTableInternal(hypertable_oid, list_make1(cmd), false);
	}
	relation_close(rel, AccessShareLock);
}

TS_FUNCTION_INFO_V1(ts_tablespace_detach);

Datum
ts_tablespace_detach(PG_FUNCTION_ARGS)
{
	Name tspcname = PG_ARGISNULL(0) ? NULL : PG_GETARG_NAME(0);
	Oid hypertable_oid = PG_ARGISNULL(1) ? InvalidOid : PG_GETARG_OID(1);
	bool if_attached = PG_ARGISNULL(2) ? false : PG_GETARG_BOOL(2);
	Oid tspcoid;
	int ret;

	TS_PREVENT_FUNC_IF_READ_ONLY();

	if (PG_NARGS() < 1 || PG_NARGS() > 3)
		elog(ERROR, "invalid number of arguments");

	if (NULL == tspcname)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid tablespace name")));

	if (!PG_ARGISNULL(1) && !OidIsValid(hypertable_oid))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid hypertable")));

	tspcoid = get_tablespace_oid(NameStr(*tspcname), true);

	if (!OidIsValid(tspcoid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("tablespace \"%s\" does not exist", NameStr(*tspcname))));

	if (OidIsValid(hypertable_oid))
	{
		ret = tablespace_detach_one(hypertable_oid, NameStr(*tspcname), tspcoid, if_attached);
		detach_tablespace_from_hypertable_if_set(hypertable_oid, tspcoid);
	}
	else
	{
		List *hypertables = NIL;
		ListCell *cell;

		ret = tablespace_delete_from_all(NameStr(*tspcname), GetUserId(), &hypertables);
		foreach (cell, hypertables)
		{
			const int32 hypertable_id = lfirst_int(cell);
			detach_tablespace_from_hypertable_if_set(ts_hypertable_id_to_relid(hypertable_id),
													 tspcoid);
		}
	}

	PG_RETURN_INT32(ret);
}

TS_FUNCTION_INFO_V1(ts_tablespace_detach_all_from_hypertable);

Datum
ts_tablespace_detach_all_from_hypertable(PG_FUNCTION_ARGS)
{
	const Oid hypertable_relid = PG_GETARG_OID(0);
	int32 result;
	AlterTableCmd *const cmd = makeNode(AlterTableCmd);

	cmd->subtype = AT_SetTableSpace;
	cmd->name = "pg_default";

	TS_PREVENT_FUNC_IF_READ_ONLY();

	if (PG_NARGS() != 1)
		elog(ERROR, "invalid number of arguments");

	if (PG_ARGISNULL(0))
		elog(ERROR, "invalid argument");

	result = tablespace_detach_all(hypertable_relid);
	AlterTableInternal(hypertable_relid, list_make1(cmd), false);

	PG_RETURN_INT32(result);
}

TS_FUNCTION_INFO_V1(ts_tablespace_show);

Datum
ts_tablespace_show(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	Oid hypertable_oid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	Cache *hcache;
	Hypertable *ht;
	Tablespaces *tspcs;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		if (!OidIsValid(hypertable_oid))
			elog(ERROR, "invalid argument");

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		funcctx->user_fctx = ts_hypertable_cache_pin();
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	hcache = funcctx->user_fctx;
	ht = ts_hypertable_cache_get_entry(hcache, hypertable_oid, CACHE_FLAG_NONE);

	tspcs = ts_tablespace_scan(ht->fd.id);

	if (NULL != tspcs && funcctx->call_cntr < (uint64) tspcs->num_tablespaces)
	{
		Oid tablespace_oid = tspcs->tablespaces[funcctx->call_cntr].tablespace_oid;
		const char *tablespace_name = get_tablespace_name(tablespace_oid);
		Datum name;

		Assert(tablespace_name != NULL);
		name = DirectFunctionCall1(namein, CStringGetDatum(tablespace_name));

		SRF_RETURN_NEXT(funcctx, name);
	}
	else
	{
		ts_cache_release(hcache);
		SRF_RETURN_DONE(funcctx);
	}
}
