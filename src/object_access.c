#include <postgres.h>
#include <catalog/objectaccess.h>
#include <catalog/objectaddress.h>
#include <catalog/pg_class.h>
#include <access/sysattr.h>
#include <access/genam.h>
#include <access/htup_details.h>
#include <utils/fmgroids.h>
#include <utils/tqual.h>
#include <utils/lsyscache.h>
#include <utils/builtins.h>

#include "object_access.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "catalog.h"
#include "extension.h"

#define rename_hypertable(old_schema_name, old_table_name, new_schema_name, new_table_name)		\
	CatalogInternalCall4(DDL_RENAME_HYPERTABLE,							\
						 DirectFunctionCall1(namein, CStringGetDatum(old_schema_name)), \
						 NameGetDatum(&old_table_name), \
						 DirectFunctionCall1(namein, CStringGetDatum(new_schema_name)), \
						 NameGetDatum(&new_table_name))


static object_access_hook_type prev_object_hook;

static HeapTuple
get_object_by_oid(Relation catalog, Oid objectId, Snapshot snapshot)
{
	HeapTuple	tuple;
	Oid			classId = RelationGetRelid(catalog);

	Oid			oidIndexId = get_object_oid_index(classId);
	SysScanDesc scan;
	ScanKeyData skey;

	Assert(OidIsValid(oidIndexId));

	ScanKeyInit(&skey,
				ObjectIdAttributeNumber,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(objectId));

	scan = systable_beginscan(catalog, oidIndexId, true,
							  snapshot, 1, &skey);
	tuple = systable_getnext(scan);
	if (!HeapTupleIsValid(tuple))
	{
		systable_endscan(scan);
		return NULL;
	}
	tuple = heap_copytuple(tuple);

	systable_endscan(scan);

	return tuple;
}


static void
alter_table(Oid relid)
{
	if (!OidIsValid(relid))
		return;

	if (is_hypertable(relid))
	{
		Relation	rel = heap_open(RelationRelationId, AccessShareLock);
		HeapTuple	oldtup = get_object_by_oid(rel, relid, NULL);
		HeapTuple	newtup = get_object_by_oid(rel, relid, SnapshotSelf);
		Form_pg_class old = (Form_pg_class) GETSTRUCT(oldtup);
		Form_pg_class new = (Form_pg_class) GETSTRUCT(newtup);

		if (strncmp(NameStr(old->relname), NameStr(new->relname), NAMEDATALEN) ||
			old->relnamespace != new->relnamespace)
		{
			const char *old_ns = get_namespace_name(old->relnamespace);
			const char *new_ns = get_namespace_name(new->relnamespace);

			rename_hypertable(old_ns, old->relname, new_ns, new->relname);
		}

		heap_close(rel, AccessShareLock);
	}
}


static void
object_access_alter_hook(Oid classId,
						 Oid objectId,
						 int subId,
						 void *arg)
{
	switch (classId)
	{
		case RelationRelationId:
			alter_table(objectId);
			break;
		default:
			break;

	}
}

static
void
timescaledb_object_access(ObjectAccessType access,
						  Oid classId,
						  Oid objectId,
						  int subId,
						  void *arg)
{
	if (prev_object_hook != NULL)
	{
		prev_object_hook(access, classId, objectId, subId, arg);
	}

	if (!extension_is_loaded())
		return;

	switch (access)
	{
		case OAT_POST_ALTER:
			object_access_alter_hook(classId, objectId, subId, arg);
			break;
		default:
			break;
	}
}

void
_object_access_init(void)
{
	prev_object_hook = object_access_hook;
	object_access_hook = timescaledb_object_access;
}

void
_object_access_fini(void)
{
	object_access_hook = prev_object_hook;
}
