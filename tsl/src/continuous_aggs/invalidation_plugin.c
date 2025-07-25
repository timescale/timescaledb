/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <fmgr.h>

#include <inttypes.h>
#include <miscadmin.h>

#include <access/attnum.h>
#include <access/genam.h>
#include <access/htup.h>
#include <access/table.h>
#include <access/transam.h>
#include <access/tupdesc.h>
#include <catalog/namespace.h>
#include <catalog/pg_attribute.h>
#include <catalog/pg_inherits.h>
#include <commands/defrem.h>
#include <executor/tuptable.h>
#include <libpq/pqformat.h>
#include <nodes/makefuncs.h>
#include <replication/logical.h>
#include <replication/output_plugin.h>
#include <storage/lockdefs.h>
#include <utils/builtins.h>
#include <utils/fmgroids.h>
#include <utils/hsearch.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/palloc.h>
#include <utils/regproc.h>
#include <utils/varlena.h>

#include "compat/compat.h"
#include "invalidation_plugin_cache.h"
#include "invalidation_record.h"
#include "utils.h"

PG_MODULE_MAGIC;

#if PG16_LT
extern TSDLLEXPORT void _PG_output_plugin_init(OutputPluginCallbacks *cb);
#endif

typedef struct TrackRelInfo
{
	Oid hypertable_relid; /* Relid of the hypertable that we're collecting
						   * invalidations for */
	AttrNumber attnum;	  /* Attribute number of the attribute to read from
						   * the chunks. */
} TrackRelInfo;

/*
 * Hash table with relations seen.
 *
 * In order to avoid multiple lookups of a relation, we record all relations
 * seen as entries in a hash table for the duration of a run (e.g., one call
 * of pg_logical_slot_get_binary_changes).
 *
 * If we see a new relation, we look up the inheritance parent and if it has
 * one, we check if it is one of the relations that we should track and set
 * the "track" field to true in that case.
 *
 * If the relation does not have an inheritance parent, or if the inheritance
 * parent is not one of the relations we track, we will set the "track" field
 * to false.
 *
 * Note that since hypertables are single-level inheritance tables, we do not
 * look further up the inheritance chain. If you want to be able to handle
 * multi-level inheritance changes, you need to update this.
 */
typedef struct SeenRelsEntry
{
	Oid relid;			   /* Relid, also key */
	bool track;			   /* Shall this relation be tracked or not? */
	TrackRelInfo *relinfo; /* Relation information, or NULL if not tracked */
} SeenRelsEntry;

typedef struct InvalidationsPluginData
{
	MemoryContext invals_context; /* Memory context for allocations for a transaction */
	HTAB *invals_cache;			  /* Invalidations cache with entries for a transaction. */
} InvalidationsPluginData;

/*
 * Hash table with relations to track.
 *
 * Might include relations that are dropped, but this is safe since it just
 * means we return invalidations for some data that does not exist any
 * more.
 *
 * The hash table and all it's entries are allocated in the logical decoding
 * memory context, which is long-lived.
 */
static HTAB *RelationSeenCache = NULL;

/*
 * Find the parent of a relid.
 *
 * This is inspired by has_superclass() in src/backend/catalog/pg_inherits.c
 * but returns the parent instead of just checking if it has a parent.
 */
static Oid
find_parent(Oid relid)
{
	ScanKeyData skey;
	Oid parent_relid = InvalidOid;
	HeapTuple inhtup;

	Relation catalog = table_open(InheritsRelationId, AccessShareLock);
	ScanKeyInit(&skey,
				Anum_pg_inherits_inhrelid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(relid));

	SysScanDesc scan = systable_beginscan(catalog, InheritsRelidSeqnoIndexId, true, NULL, 1, &skey);
	if ((inhtup = systable_getnext(scan)) != NULL)
	{
		Form_pg_inherits inh = (Form_pg_inherits) GETSTRUCT(inhtup);
		parent_relid = inh->inhparent;
	}
	systable_endscan(scan);
	table_close(catalog, AccessShareLock);

	return parent_relid;
}

/*
 * Find all inheritance children of a relation and store in a hash table.
 *
 * This is a simplified version of find_all_inheritors from pg_inherits.c
 * intended for hypertables only.
 *
 * Note that we are not locking the relids once we've found them.
 */
static void
add_inheritance_children(HTAB *rels, TrackRelInfo *relinfo)
{
	List *children = find_inheritance_children(relinfo->hypertable_relid, NoLock);
	children = lappend_oid(children, relinfo->hypertable_relid);
	ListCell *lc;
	foreach (lc, children)
	{
		Oid child_oid = lfirst_oid(lc);

		bool found;
		SeenRelsEntry *entry = hash_search(rels, &child_oid, HASH_ENTER, &found);

		/* We shouldn't have duplicates, but in case we do, we just skip them */
		if (!found)
		{
			entry->relid = child_oid;
			entry->track = true;
			entry->relinfo = relinfo;
		}
	}
	pfree(children);
}

/*
 * Check if relation should be tracked.
 *
 * A relation should be tracked either if it is in the cache and is marked as
 * being tracked, or it has a parent that is tracked.
 *
 * This will also update the hash of relations that we have seen.
 *
 * The relinfo field is set to the relation information for the relation it
 * should be tracked under, i.e., the hypertable.
 */
static bool
is_relation_tracked(Oid relid, TrackRelInfo **relinfo_var)
{
	bool found;
	Assert(RelationSeenCache != NULL);
	SeenRelsEntry *entry = hash_search(RelationSeenCache, &relid, HASH_ENTER, &found);

	if (!found)
	{
		Oid parent_relid = find_parent(relid);
		if (OidIsValid(parent_relid))
		{
			SeenRelsEntry *parent_entry =
				hash_search(RelationSeenCache, &parent_relid, HASH_FIND, &found);
			entry->track = found;
			entry->relinfo = found ? parent_entry->relinfo : NULL;
		}
		else
		{
			entry->track = false;
		}
	}

	*relinfo_var = entry->relinfo;

	return entry->track;
}

#if PG_VERSION_NUM < 170000
static HeapTuple
as_tuple(ReorderBufferTupleBuf *buf)
{
	return &buf->tuple;
}
#else
static HeapTuple
as_tuple(HeapTuple tuple)
{
	return tuple;
}
#endif

static void
init_rel_seen_cache(List *relinfo_list)
{
	HASHCTL ctl = {
		.keysize = sizeof(Oid),
		.entrysize = sizeof(SeenRelsEntry),
		.hcxt = CurrentMemoryContext,
	};

	if (RelationSeenCache)
		return;

	RelationSeenCache = hash_create("relations for invalidations plugin",
									128,
									&ctl,
									HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	ListCell *lc;
	foreach (lc, relinfo_list)
	{
		TrackRelInfo *track_relinfo = lfirst(lc);
		add_inheritance_children(RelationSeenCache, track_relinfo);
	}
}

static void
invalidations_insert_tuple(InvalidationsPluginData *data, TrackRelInfo *relinfo, Relation rel,
						   HeapTuple tuple)
{
	TupleDesc tupdesc = RelationGetDescr(rel);
	Form_pg_attribute attr = TupleDescAttr(tupdesc, relinfo->attnum - 1);
	Assert(relinfo->attnum != InvalidAttrNumber);

	/*
	 * It might be that the attribute is dropped somewhere in the stream we
	 * are reading, but for that case, the attribute number will not change,
	 * it will just be set as dropped, so we will just ignore it.
	 *
	 * Note that this is expected to be a partitioning attribute, and it
	 * should not be possible to drop a partitioning attribute, but for the
	 * unexpected case that we in non-debug-built code get a tuple that have a
	 * dropped partitioning attribute, is seems excessive to raise an error
	 * when we can just ignore the tuple.
	 */
	Assert(!attr->attisdropped);
	if (attr->attisdropped)
		return;

	bool isnull;
	Datum datum = heap_getattr(tuple, relinfo->attnum, tupdesc, &isnull);

	Assert(!isnull);
	if (isnull)
		return;

	invalidation_cache_record_entry(data->invals_cache,
									relinfo->hypertable_relid,
									ts_time_value_to_internal(datum, attr->atttypid));
}

static void
invalidations_record_insert(InvalidationsPluginData *data, TrackRelInfo *relinfo,
							LogicalDecodingContext *ctx, Relation relation,
							ReorderBufferChange *change)
{
	invalidations_insert_tuple(data, relinfo, relation, as_tuple(change->data.tp.newtuple));
}

static void
invalidations_record_update(InvalidationsPluginData *data, TrackRelInfo *relinfo,
							LogicalDecodingContext *ctx, Relation relation,
							ReorderBufferChange *change)
{
	/*
	 * The field oldtuple is only set if any of the columns in the replica
	 * identity are modified. Otherwise, oldtuple is set to NULL.
	 *
	 * This means that if oldtuple is NULL, it is sufficient to use newtuple
	 * to decide what range that was updated.
	 */
	if (change->data.tp.oldtuple)
		invalidations_insert_tuple(data, relinfo, relation, as_tuple(change->data.tp.oldtuple));
	invalidations_insert_tuple(data, relinfo, relation, as_tuple(change->data.tp.newtuple));
}

static void
invalidations_record_delete(InvalidationsPluginData *data, TrackRelInfo *relinfo,
							LogicalDecodingContext *ctx, Relation relation,
							ReorderBufferChange *change)
{
	/*
	 * We need a replica identity to deal with deletes. If the table does not
	 * have a replica identity, we would have to invalidate the entire table,
	 * so instead we throw an error if the delete does not have an oldtuple.
	 */
	if (!change->data.tp.oldtuple)
		ereport(ERROR,
				errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				errmsg("table is missing replica identity"),
				errdetail("Table \"%s\" need to have a replica identity to handle DELETE.",
						  RelationGetRelationName(relation)));
	invalidations_insert_tuple(data, relinfo, relation, as_tuple(change->data.tp.oldtuple));
}

static List *
parse_output_parameters(List *options)
{
	ListCell *option;
	List *relinfo_list = NIL;

	/*
	 * Options are pairs of relation names and the attribute to read.
	 *
	 * We do not check if the "key" is a hypertable since we do not want to
	 * rely on the TimescaleDB metadata.
	 */
	foreach (option, options)
	{
		DefElem *elem = lfirst(option);
		const char *name = elem->defname;
#if PG16_LT
		List *qname = stringToQualifiedNameList(name);
#else
		List *qname = stringToQualifiedNameList(name, NULL);
#endif
		TrackRelInfo *relinfo = palloc0(sizeof(TrackRelInfo));
		RangeVar *hypertable_rv = makeRangeVarFromNameList(qname);
		relinfo->hypertable_relid = RangeVarGetRelid(hypertable_rv, AccessShareLock, true);
		if (!OidIsValid(relinfo->hypertable_relid))
			ereport(ERROR,
					errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("relation \"%s\" does not exist", name));

		NameData attname;
		namestrcpy(&attname, strVal(elem->arg));

		relinfo->attnum = get_attnum(relinfo->hypertable_relid, NameStr(attname));
		if (relinfo->attnum == InvalidAttrNumber)
			ereport(ERROR,
					errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("attribute \"%s\" does not exist", NameStr(attname)));

		relinfo_list = lappend(relinfo_list, relinfo);
	}
	return relinfo_list;
}

static void
invalidations_write_record(InvalidationCacheEntry *entry, bool last_write,
						   InvalidationsContext *args)
{
	LogicalDecodingContext *ctx = args->ctx;

	TS_DEBUG_LOG("write entry: hypertable=\"%s\", lowest_modified=" UINT64_FORMAT
				 ", highest_modified=" UINT64_FORMAT,
				 get_rel_name(entry->hypertable_relid),
				 entry->lowest_modified,
				 entry->highest_modified);

	InvalidationMessage msg;
	msg.ver1.hypertable_relid = entry->hypertable_relid;
	msg.ver1.lowest_modified = entry->lowest_modified;
	msg.ver1.highest_modified = entry->highest_modified;

	OutputPluginPrepareWrite(ctx, last_write);
	ts_invalidation_record_encode(&msg, ctx->out);
	OutputPluginWrite(ctx, last_write);
}

static void
invalidations_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init)
{
	InvalidationsPluginData *data = palloc0(sizeof(InvalidationsPluginData));

	TS_DEBUG_LOG("startup in memory context \"%s\"", ctx->context->name);

	data->invals_context =
		AllocSetContextCreate(ctx->context,
							  "Continuous aggregates invalidations memory context",
							  ALLOCSET_DEFAULT_SIZES);
	ctx->output_plugin_private = data;

	opt->output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;

	if (!is_init)
	{
		TS_DEBUG_LOG("current memory context when parsing parameters: \"%s\"",
					 CurrentMemoryContext->name);
		List *relinfo_list = parse_output_parameters(ctx->output_plugin_options);
		init_rel_seen_cache(relinfo_list);
	}
}

static void
invalidations_shutdown(LogicalDecodingContext *ctx)
{
	InvalidationsPluginData *data = ctx->output_plugin_private;

	MemoryContextDelete(data->invals_context);

	if (RelationSeenCache)
	{
		hash_destroy(RelationSeenCache);
		RelationSeenCache = NULL;
	}
}

/*
 * BEGIN callback: set up the cache to collect the invalidation
 * ranges inside the transaction.
 *
 * This is similar to how the execute_cagg_trigger function works, which
 * collects invalidation entries on a per-transaction basis.
 */
static void
invalidations_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
	InvalidationsPluginData *data = ctx->output_plugin_private;
	Assert(data->invals_cache == NULL);
	data->invals_cache = invalidation_cache_create(data->invals_context);
}

/*
 * COMMIT callback that will write the collected ranges to the output using
 * logical replication format.
 */
static void
invalidations_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, XLogRecPtr commit_lsn)
{
	InvalidationsPluginData *data = ctx->output_plugin_private;

	invalidation_cache_foreach_entry(data->invals_cache,
									 invalidations_write_record,
									 &(InvalidationsContext){ .ctx = ctx, .txn = txn });

	invalidation_cache_destroy(data->invals_cache);
	data->invals_cache = NULL;
}

/*
 * BEGIN PREPARE callback. Nothing to do here.
 */
static void
invalidations_begin_prepare_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
}

/*
 * PREPARE callback. Nothing to do here.
 */
static void
invalidations_prepare_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
						  XLogRecPtr prepare_lsn)
{
}

/*
 * ROLLBACK PREPARED callback
 */
static void
invalidations_rollback_prepared_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
									XLogRecPtr prepare_end_lsn, TimestampTz prepare_time)
{
}

static bool
invalidations_filter_prepare(LogicalDecodingContext *ctx, TransactionId xid, const char *gid)
{
	return false;
}

/* Change callback. */
static void
invalidations_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, Relation relation,
					 ReorderBufferChange *change)
{
	InvalidationsPluginData *data = ctx->output_plugin_private;
	Oid relid = RelationGetRelid(relation);

	TrackRelInfo *track_relinfo = NULL;
	if (!is_relation_tracked(relid, &track_relinfo))
		return;

	Assert(track_relinfo != NULL);

	MemoryContext old = MemoryContextSwitchTo(data->invals_context);
	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			invalidations_record_insert(data, track_relinfo, ctx, relation, change);
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			invalidations_record_update(data, track_relinfo, ctx, relation, change);
			break;
		case REORDER_BUFFER_CHANGE_DELETE:
			invalidations_record_delete(data, track_relinfo, ctx, relation, change);
			break;
		default:
			Assert(false);
	}
	MemoryContextSwitchTo(old);
}

void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	memset(cb, 0, sizeof(OutputPluginCallbacks));
	cb->startup_cb = invalidations_startup;
	cb->begin_cb = invalidations_begin_txn;
	cb->change_cb = invalidations_change;
	cb->commit_cb = invalidations_commit_txn;
	cb->shutdown_cb = invalidations_shutdown;
	cb->filter_prepare_cb = invalidations_filter_prepare;
	cb->prepare_cb = invalidations_prepare_txn;
	cb->begin_prepare_cb = invalidations_begin_prepare_txn;
	cb->rollback_prepared_cb = invalidations_rollback_prepared_txn;
}
