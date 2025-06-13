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
#include <nodes/makefuncs.h>
#include <replication/logical.h>
#include <replication/logicalproto.h>
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

#include "export.h"
#include "invalidation_cache.h"
#include "ts_catalog/catalog.h"

PG_MODULE_MAGIC;

#if PG16_LT
extern TSDLLEXPORT void _PG_output_plugin_init(OutputPluginCallbacks *cb);
#endif

/*
 * Hash table with relations seen.
 *
 * In order to avoid multiple lookups of a relation, we record all relations
 * seen as entries in a hash table.
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
	Oid relid;	/* Relid, also key */
	Oid parent; /* Parent relid, if there is one */
	bool track; /* Shall this relation be tracked or not? */
} SeenRelsEntry;

typedef struct InvalidationsPluginData
{
	MemoryContext context;
	RangeVar *hypertable_rv;
	Oid hypertable_relid; /* Relid of the hypertable that we're collecting
						   * invalidations for */
	Relation logrel;	  /* The log relation used for tuples passed back to
						   * the caller. */
	NameData attname;	  /* Name of the attribute to read from the chunks */
	int attnum;			  /* Attribute number of the attribute to read from
						   * the chunks. */
	HTAB *invals_cache;	  /* Invalidations cache, for each transaction. */
} InvalidationsPluginData;

/*
 * Hash table with relations to track.
 *
 * Might include relations that are dropped, but this is safe since it just
 * means we return invalidations for some data that does not exist any
 * more. This is allocated in the cache context above, which is long-lived.
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
add_inheritance_children(HTAB *rels, Oid hypertable_relid)
{
	List *children = find_inheritance_children(hypertable_relid, NoLock);
	children = lappend_oid(children, hypertable_relid);
	ListCell *lc;
	foreach (lc, children)
	{
		Oid child_oid = lfirst_oid(lc);
		SeenRelsEntry *entry;
		bool found;

		entry = hash_search(rels, &child_oid, HASH_ENTER, &found);
		if (!found)
			entry->track = true;
	}
}

/*
 * Check if relation should be tracked.
 *
 * A relation should be tracked either if it is in the cache and is marked as
 * being tracked, or it has a parent that is tracked.
 *
 * This will also update the hash of relations that we have seen.
 */
static bool
is_relation_tracked(InvalidationsPluginData *data, Oid relid)
{
	bool found;
	Assert(RelationSeenCache != NULL);
	SeenRelsEntry *entry = hash_search(RelationSeenCache, &relid, HASH_ENTER, &found);

	if (!found)
	{
		Oid parent_relid = find_parent(relid);
		if (OidIsValid(parent_relid))
		{
			hash_search(RelationSeenCache, &parent_relid, HASH_FIND, &found);
			entry->track = found;
			entry->parent = parent_relid;
		}
		else
			entry->track = false;
	}

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
init_rel_seen_cache(Oid hypertable_relid)
{
	HASHCTL ctl = {
		.keysize = sizeof(Oid),
		.entrysize = sizeof(SeenRelsEntry),
		.hcxt = CurrentMemoryContext,
	};

	Assert(OidIsValid(hypertable_relid));

	if (RelationSeenCache)
		return;

	RelationSeenCache = hash_create("relations for invalidations plugin",
									128,
									&ctl,
									HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	add_inheritance_children(RelationSeenCache, hypertable_relid);
}

static void
invalidations_insert_tuple(InvalidationsPluginData *data, Relation rel, HeapTuple tuple)
{
	TupleDesc tupdesc = RelationGetDescr(rel);
	Form_pg_attribute attr = TupleDescAttr(tupdesc, data->attnum - 1);
	Assert(data->attnum != InvalidAttrNumber);

	/*
	 * It might be that the attribute is dropped somewhere in the stream we
	 * are reading, but for that case, the attribute number will not change,
	 * it will just be set as dropped, so we will just ignore it.
	 */
	Assert(!attr->attisdropped);
	if (attr->attisdropped)
		return;

	bool isnull;
	Datum datum = heap_getattr(tuple, data->attnum, tupdesc, &isnull);

	Assert(!isnull);
	if (isnull)
		return;

	invalidation_cache_write_record(data->invals_cache,
									data->hypertable_relid,
									ts_time_value_to_internal(datum, attr->atttypid));
}

static void
invalidations_record_insert(InvalidationsPluginData *data, LogicalDecodingContext *ctx,
							Relation relation, ReorderBufferChange *change)
{
	invalidations_insert_tuple(data, relation, as_tuple(change->data.tp.newtuple));
}

static void
invalidations_record_update(InvalidationsPluginData *data, LogicalDecodingContext *ctx,
							Relation relation, ReorderBufferChange *change)
{
	if (change->data.tp.oldtuple)
		invalidations_insert_tuple(data, relation, as_tuple(change->data.tp.oldtuple));
	invalidations_insert_tuple(data, relation, as_tuple(change->data.tp.newtuple));
}

static void
invalidations_record_delete(InvalidationsPluginData *data, LogicalDecodingContext *ctx,
							Relation relation, ReorderBufferChange *change)
{
	Ensure(change->data.tp.oldtuple, "table is missing replica identity");
	invalidations_insert_tuple(data, relation, as_tuple(change->data.tp.oldtuple));
}

static void
parse_output_parameters(List *options, InvalidationsPluginData *data)
{
	ListCell *option;

	foreach (option, options)
	{
		DefElem *elem = lfirst(option);
		if (strcmp(elem->defname, "hypertable") == 0)
		{
			const char *name = strVal(elem->arg);
#if PG16_LT
			List *qname = stringToQualifiedNameList(name);
#else
			List *qname = stringToQualifiedNameList(name, NULL);
#endif
			data->hypertable_rv = makeRangeVarFromNameList(qname);
			data->hypertable_relid = RangeVarGetRelid(data->hypertable_rv, AccessShareLock, true);
			if (!OidIsValid(data->hypertable_relid))
				ereport(ERROR,
						errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("hypertable \"%s\" does not exist", name));
		}
		else if (strcmp(elem->defname, "attribute") == 0)
			namestrcpy(&data->attname, strVal(elem->arg));
	}
}

static void
invalidations_write_record(HypertableInvalidationCacheEntry *entry, bool last_write,
						   InvalidationsContext *args)
{
	LogicalDecodingContext *ctx = args->ctx;
	InvalidationsPluginData *data = ctx->output_plugin_private;
	TupleDesc tupdesc = RelationGetDescr(data->logrel);
	Datum values[_Anum_continuous_aggs_hypertable_invalidation_log_max];
	bool isnull[_Anum_continuous_aggs_hypertable_invalidation_log_max] = { false };

	TS_DEBUG_LOG("write entry: hypertable_id=%u, lowest_modified_value=%" PRId64 ", "
				 "greatest_modified_value=%" PRId64,
				 entry->hypertable_relid,
				 entry->lowest_modified_value,
				 entry->greatest_modified_value);

	/* Copy over the field to the invalidation slot */
	values[AttrNumberGetAttrOffset(
		Anum_continuous_aggs_hypertable_invalidation_log_hypertable_id)] =
		ObjectIdGetDatum(entry->hypertable_relid);
	values[AttrNumberGetAttrOffset(
		Anum_continuous_aggs_hypertable_invalidation_log_lowest_modified_value)] =
		Int64GetDatum(entry->lowest_modified_value);
	values[AttrNumberGetAttrOffset(
		Anum_continuous_aggs_hypertable_invalidation_log_greatest_modified_value)] =
		Int64GetDatum(entry->greatest_modified_value);

	MinimalTuple tuple = heap_form_minimal_tuple(tupdesc, values, isnull);
	ExecStoreMinimalTuple(tuple, args->slot, true);

	OutputPluginPrepareWrite(ctx, last_write);
	/* Since we are not writing streamed data, we do not pass in an XID */
	logicalrep_write_insert(ctx->out,
							InvalidTransactionId,
							data->logrel,
							args->slot,
							true, /* binary */
							NULL);
	OutputPluginWrite(ctx, last_write);
}

static void
invalidations_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init)
{
	InvalidationsPluginData *data = palloc0(sizeof(InvalidationsPluginData));

	data->context = AllocSetContextCreate(ctx->context,
										  "Continuous aggregates invalidations memory context",
										  ALLOCSET_DEFAULT_SIZES);
	ctx->output_plugin_private = data;

	opt->output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;

	if (!is_init)
	{
		parse_output_parameters(ctx->output_plugin_options, data);

		/*
		 * We read the attribute number during startup, which means that we
		 * get the attribute number for the definition of the table when we
		 * read invalidations.
		 */
		data->attnum = get_attnum(data->hypertable_relid, NameStr(data->attname));
		if (data->attnum == InvalidAttrNumber)
			ereport(ERROR,
					errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("attribute \"%s\" does not exist", NameStr(data->attname)));

		data->logrel =
			table_openrv(makeRangeVar(CATALOG_SCHEMA_NAME,
									  CONTINUOUS_AGGS_HYPERTABLE_INVALIDATION_LOG_TABLE_NAME,
									  -1),
						 AccessShareLock);

		init_rel_seen_cache(data->hypertable_relid);
	}
}

static void
invalidations_shutdown(LogicalDecodingContext *ctx)
{
	InvalidationsPluginData *data = ctx->output_plugin_private;

	if (data->logrel)
		table_close(data->logrel, NoLock);

	MemoryContextDelete(data->context);

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
	data->invals_cache = invalidation_cache_create(data->context);
}

/*
 * COMMIT callback that will write the collected ranges to the output using
 * logical replication format.
 */
static void
invalidations_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, XLogRecPtr commit_lsn)
{
	InvalidationsPluginData *data = ctx->output_plugin_private;
	TupleDesc tupdesc = RelationGetDescr(data->logrel);
	TupleTableSlot *slot = MakeSingleTupleTableSlot(tupdesc, &TTSOpsMinimalTuple);

	invalidation_cache_foreach_record(data->invals_cache,
									  invalidations_write_record,
									  &(InvalidationsContext){
										  .ctx = ctx, .txn = txn, .slot = slot });

	ExecDropSingleTupleTableSlot(slot);
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

	if (!is_relation_tracked(data, relid))
		return;

	MemoryContext old = MemoryContextSwitchTo(data->context);
	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			invalidations_record_insert(data, ctx, relation, change);
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			invalidations_record_update(data, ctx, relation, change);
			break;
		case REORDER_BUFFER_CHANGE_DELETE:
			invalidations_record_delete(data, ctx, relation, change);
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
