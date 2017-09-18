#include <postgres.h>
#include <catalog/objectaccess.h>
#include <catalog/objectaddress.h>
#include <catalog/pg_class.h>
#include <catalog/pg_constraint.h>
#include <catalog/indexing.h>
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
#include "process_utility.h"

#define rename_hypertable(old_schema_name, old_table_name, new_schema_name, new_table_name)		\
	CatalogInternalCall4(DDL_RENAME_HYPERTABLE,							\
						 DirectFunctionCall1(namein, CStringGetDatum(old_schema_name)), \
						 NameGetDatum(&old_table_name), \
						 DirectFunctionCall1(namein, CStringGetDatum(new_schema_name)), \
						 NameGetDatum(&new_table_name))

#define add_hypertable_constraint(hypertable, constraint_name) \
	CatalogInternalCall2(DDL_ADD_CONSTRAINT,					\
						 Int32GetDatum((hypertable)->fd.id),		   \
						 NameGetDatum(&constraint_name))

#define rename_hypertable_column(hypertable, old_name, new_name) \
	CatalogInternalCall3(DDL_RENAME_COLUMN,								\
						 Int32GetDatum((hypertable)->fd.id),			\
						 NameGetDatum(&old_name),						\
						 NameGetDatum(&new_name))

#define change_hypertable_column_type(hypertable, column_name, new_type) \
	CatalogInternalCall3(DDL_CHANGE_COLUMN_TYPE,						\
						 Int32GetDatum((hypertable)->fd.id), \
						 NameGetDatum(&column_name), \
						 ObjectIdGetDatum(new_type))


static object_access_hook_type prev_object_hook;

static HeapTuple
get_attribute_by_relid_attnum(Relation catalog, Oid relid, int16 attnum, Snapshot snapshot)
{
	HeapTuple	tuple;
	SysScanDesc scan;
	ScanKeyData skey[2];

	ScanKeyInit(&skey[0],
				Anum_pg_attribute_attrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));
	ScanKeyInit(&skey[1],
				Anum_pg_attribute_attnum,
				BTEqualStrategyNumber, F_INT2EQ,
				Int16GetDatum(attnum));

	scan = systable_beginscan(catalog, AttributeRelidNumIndexId, true,
							  snapshot, 2, skey);
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

		if (strncmp(NameStr(old->relname), NameStr(new->relname), NAMEDATALEN) != 0 ||
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
alter_column(Oid relid, int16 attnum)
{
	Relation	rel = heap_open(AttributeRelationId, AccessShareLock);
	HeapTuple	oldtup = get_attribute_by_relid_attnum(rel, relid, attnum, NULL);
	Form_pg_attribute old = (Form_pg_attribute) GETSTRUCT(oldtup);

	if (!old->attisdropped && old->attnum > 0)
	{
		Cache	   *hcache = hypertable_cache_pin();
		Hypertable *ht = hypertable_cache_get_entry(hcache, old->attrelid);

		if (ht != NULL)
		{
			HeapTuple	newtup = get_attribute_by_relid_attnum(rel, relid, attnum, SnapshotSelf);
			Form_pg_attribute new = (Form_pg_attribute) GETSTRUCT(newtup);

			if (strncmp(NameStr(old->attname), NameStr(new->attname), NAMEDATALEN) != 0)
			{
				CatalogSecurityContext sec_ctx;

				catalog_become_owner(catalog_get(), &sec_ctx);
				rename_hypertable_column(ht, old->attname, new->attname);
				catalog_restore_user(&sec_ctx);
			}
			if (old->atttypid != new->atttypid)
			{
				CatalogSecurityContext sec_ctx;

				catalog_become_owner(catalog_get(), &sec_ctx);
				process_utility_set_expect_chunk_modification(true);
				change_hypertable_column_type(ht, new->attname, new->atttypid);
				process_utility_set_expect_chunk_modification(false);
				catalog_restore_user(&sec_ctx);
			}
		}
		cache_release(hcache);
	}

	heap_close(rel, AccessShareLock);
}

static void
create_constraint(Oid constraint_oid)
{
	Relation	rel;

	if (!OidIsValid(constraint_oid))
		return;

	rel = heap_open(ConstraintRelationId, AccessShareLock);

	HeapTuple	newtup = get_object_by_oid(rel, constraint_oid, SnapshotSelf);
	Form_pg_constraint new = (Form_pg_constraint) GETSTRUCT(newtup);

	if (OidIsValid(new->conrelid))
	{
		Cache	   *hcache = hypertable_cache_pin();
		Hypertable *ht = hypertable_cache_get_entry(hcache, new->conrelid);

		if (ht != NULL)
		{
			Assert(strlen(new->conname.data) > 0);
			process_utility_set_expect_chunk_modification(true);
			add_hypertable_constraint(ht, new->conname);
			process_utility_set_expect_chunk_modification(false);
		}
		else
		{
			if (new->contype == CONSTRAINT_FOREIGN)
			{
				Hypertable *foreign = hypertable_cache_get_entry(hcache, new->confrelid);

				if (foreign != NULL)
				{
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("Foreign keys to hypertables are not supported.")));
				}
			}
		}

		cache_release(hcache);
	}
	heap_close(rel, AccessShareLock);
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
			if (subId == 0)
				alter_table(objectId);
			else
				alter_column(objectId, subId);
			break;
		default:
			break;

	}
}

static void
object_access_create_hook(Oid classId,
						  Oid objectId,
						  int subId,
						  void *arg)
{
	switch (classId)
	{
		case ConstraintRelationId:
			create_constraint(objectId);
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
		case OAT_POST_CREATE:
			object_access_create_hook(classId, objectId, subId, arg);
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
