#include <postgres.h>
#include <catalog/objectaccess.h>
#include <catalog/objectaddress.h>
#include <catalog/pg_class.h>
#include <catalog/pg_constraint.h>
#include <catalog/pg_constraint_fn.h>
#include <catalog/pg_authid.h>
#include <catalog/indexing.h>
#include <catalog/index.h>
#include <access/sysattr.h>
#include <access/genam.h>
#include <access/htup_details.h>
#include <utils/fmgroids.h>
#include <utils/tqual.h>
#include <utils/lsyscache.h>
#include <utils/builtins.h>
#include <utils/rel.h>
#include <access/heapam.h>

#include "object_access.h"
#include "extension.h"
#include "hypertable_cache.h"
#include "cache.h"
#include "chunk.h"
#include "chunk_index.h"
#include "chunk_constraint.h"

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
drop_chunk_constraint(Hypertable *ht, Oid chunk_relid, void *arg)
{
	char	   *hypertable_constraint_name = arg;
	Chunk	   *chunk = chunk_get_by_relid(chunk_relid, ht->space->num_dimensions, true);

	chunk_constraint_drop_by_hypertable_constraint_name(chunk->fd.id, chunk->table_id, hypertable_constraint_name);
}

static void
drop_constraint(Oid constraint_oid)
{
	Cache	   *hcache = hypertable_cache_pin();
	Hypertable *ht;
	Relation	rel = heap_open(ConstraintRelationId, AccessShareLock);


	HeapTuple	constrainttup = get_object_by_oid(rel, constraint_oid, SnapshotSelf);
	Form_pg_constraint constraint = (Form_pg_constraint) GETSTRUCT(constrainttup);

	ht = hypertable_cache_get_entry(hcache, constraint->conrelid);
	if (ht != NULL)
	{
		CatalogSecurityContext sec_ctx;

		/* Note cannot drop related indexes here since they are dropped first */

		catalog_become_owner(catalog_get(), &sec_ctx);

		/* Recurse to each chunk and drop the corresponding constraint */
		hypertable_foreach_chunk(ht, drop_chunk_constraint, constraint->conname.data);

		catalog_restore_user(&sec_ctx);
	}
	else
	{
		Chunk	   *chunk = chunk_get_by_relid(constraint->conrelid, 0, false);

		if (NULL != chunk)
		{
			/* drop corresponding chunk metadata */
			chunk_constraint_delete_by_constraint_oid(chunk->fd.id, constraint_oid);
		}
	}

	heap_close(rel, AccessShareLock);
	cache_release(hcache);
}

static void
drop_index(Oid idxrelid)
{
	Cache	   *hcache = hypertable_cache_pin();
	Oid			tblrelid = IndexGetRelation(idxrelid, false);
	Hypertable *ht;

	ht = hypertable_cache_get_entry(hcache, tblrelid);
	if (NULL != ht)
	{
		Oid			constraint_oid = get_index_constraint(idxrelid);

		/*
		 * Only drop the index if there is no associated constraint.
		 * Constraint drops will take care of this. Note inside PG logic an
		 * index dropped before a constraint
		 */
		if (!OidIsValid(constraint_oid))
			chunk_index_drop_children_of(ht, idxrelid);
	}
	else
	{
		Chunk	   *chunk = chunk_get_by_relid(tblrelid, 0, false);

		if (NULL != chunk)
		{
			chunk_index_delete(chunk, idxrelid, false);
		}
	}

	cache_release(hcache);
}

static void
drop_relation(Oid class_id)
{
	Relation	rel = heap_open(RelationRelationId, AccessShareLock);
	HeapTuple	relationtup = get_object_by_oid(rel, class_id, SnapshotSelf);
	Form_pg_class class = (Form_pg_class) GETSTRUCT(relationtup);

	switch (class->relkind)
	{
		case 'i':
			drop_index(class_id);
			break;
		default:
			break;
	}
	heap_close(rel, AccessShareLock);
}

static
void
object_access_drop(Oid classId,
				   Oid objectId,
				   int subId,
				   void *arg)
{
	ObjectAccessDrop *drop_arg;

	drop_arg = arg;

	/*
	 * A good example of  PERFORM_DELETION_INTERNAL is what happens when a
	 * column type changes. In that case constraints are deleted and
	 * recreated: both in PG and in TimescaleDB code. when that happens
	 * PERFORM_DELETION_INTERNAL is set and the operation should be ignored
	 */
	if (drop_arg->dropflags & PERFORM_DELETION_INTERNAL)
		return;

	switch (classId)
	{
		case ConstraintRelationId:
			drop_constraint(objectId);
			break;
		case RelationRelationId:
			drop_relation(objectId);
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
		case OAT_DROP:
			object_access_drop(classId, objectId, subId, arg);
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
