/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/*
 * This file handles commands on continuous aggs that should be allowed in
 * apache only mode. Right now this consists mostly of drop commands
 */

#include <postgres.h>
#include <access/htup_details.h>
#include <access/xact.h>
#include <catalog/dependency.h>
#include <catalog/namespace.h>
#include <catalog/pg_trigger.h>
#include <commands/trigger.h>
#include <fmgr.h>
#include <storage/lmgr.h>
#include <utils/acl.h>
#include <utils/builtins.h>
#include <utils/date.h>
#include <utils/lsyscache.h>
#include <utils/timestamp.h>
#include <miscadmin.h>

#include "compat/compat.h"

#include "bgw/job.h"
#include "continuous_agg.h"
#include "hypertable.h"
#include "scan_iterator.h"
#include "time_bucket.h"
#include "time_utils.h"
#include "catalog.h"

#define CHECK_NAME_MATCH(name1, name2) (namestrcmp(name1, name2) == 0)

static const WithClauseDefinition continuous_aggregate_with_clause_def[] = {
		[ContinuousEnabled] = {
			.arg_name = "continuous",
			.type_id = BOOLOID,
			.default_val = BoolGetDatum(false),
		},
		[ContinuousViewOptionCreateGroupIndex] = {
			.arg_name = "create_group_indexes",
			.type_id = BOOLOID,
			.default_val = BoolGetDatum(true),
		},
		[ContinuousViewOptionMaterializedOnly] = {
			.arg_name = "materialized_only",
			.type_id = BOOLOID,
			.default_val = BoolGetDatum(false),
		},
        [ContinuousViewOptionCompress] = {
			.arg_name = "compress",
			.type_id = BOOLOID,
			.default_val = BoolGetDatum(false),
		},
};

WithClauseResult *
ts_continuous_agg_with_clause_parse(const List *defelems)
{
	return ts_with_clauses_parse(defelems,
								 continuous_aggregate_with_clause_def,
								 TS_ARRAY_LEN(continuous_aggregate_with_clause_def));
}
static void
init_scan_by_mat_hypertable_id(ScanIterator *iterator, const int32 mat_hypertable_id)
{
	iterator->ctx.index = catalog_get_index(ts_catalog_get(), CONTINUOUS_AGG, CONTINUOUS_AGG_PKEY);

	ts_scan_iterator_scan_key_init(iterator,
								   Anum_continuous_agg_pkey_mat_hypertable_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(mat_hypertable_id));
}

static void
init_scan_by_raw_hypertable_id(ScanIterator *iterator, const int32 raw_hypertable_id)
{
	iterator->ctx.index =
		catalog_get_index(ts_catalog_get(), CONTINUOUS_AGG, CONTINUOUS_AGG_RAW_HYPERTABLE_ID_IDX);

	ts_scan_iterator_scan_key_init(iterator,
								   Anum_continuous_agg_raw_hypertable_id_idx_raw_hypertable_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(raw_hypertable_id));
}

static void
init_invalidation_threshold_scan_by_hypertable_id(ScanIterator *iterator,
												  const int32 raw_hypertable_id)
{
	iterator->ctx.index = catalog_get_index(ts_catalog_get(),
											CONTINUOUS_AGGS_INVALIDATION_THRESHOLD,
											CONTINUOUS_AGGS_INVALIDATION_THRESHOLD_PKEY);

	ts_scan_iterator_scan_key_init(iterator,
								   Anum_continuous_aggs_invalidation_threshold_pkey_hypertable_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(raw_hypertable_id));
}

static void
init_hypertable_invalidation_log_scan_by_hypertable_id(ScanIterator *iterator,
													   const int32 raw_hypertable_id)
{
	iterator->ctx.index = catalog_get_index(ts_catalog_get(),
											CONTINUOUS_AGGS_HYPERTABLE_INVALIDATION_LOG,
											CONTINUOUS_AGGS_HYPERTABLE_INVALIDATION_LOG_IDX);

	ts_scan_iterator_scan_key_init(
		iterator,
		Anum_continuous_aggs_hypertable_invalidation_log_idx_hypertable_id,
		BTEqualStrategyNumber,
		F_INT4EQ,
		Int32GetDatum(raw_hypertable_id));
}

static void
init_materialization_invalidation_log_scan_by_materialization_id(ScanIterator *iterator,
																 const int32 materialization_id)
{
	iterator->ctx.index = catalog_get_index(ts_catalog_get(),
											CONTINUOUS_AGGS_MATERIALIZATION_INVALIDATION_LOG,
											CONTINUOUS_AGGS_MATERIALIZATION_INVALIDATION_LOG_IDX);

	ts_scan_iterator_scan_key_init(
		iterator,
		Anum_continuous_aggs_materialization_invalidation_log_idx_materialization_id,
		BTEqualStrategyNumber,
		F_INT4EQ,
		Int32GetDatum(materialization_id));
}

static int32
number_of_continuous_aggs_attached(int32 raw_hypertable_id)
{
	ScanIterator iterator =
		ts_scan_iterator_create(CONTINUOUS_AGG, AccessShareLock, CurrentMemoryContext);
	int32 count = 0;

	init_scan_by_raw_hypertable_id(&iterator, raw_hypertable_id);
	ts_scanner_foreach(&iterator) { count++; }
	return count;
}

static void
invalidation_threshold_delete(int32 raw_hypertable_id)
{
	ScanIterator iterator = ts_scan_iterator_create(CONTINUOUS_AGGS_INVALIDATION_THRESHOLD,
													RowExclusiveLock,
													CurrentMemoryContext);

	init_invalidation_threshold_scan_by_hypertable_id(&iterator, raw_hypertable_id);

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));
	}
}

static void
hypertable_invalidation_log_delete(int32 raw_hypertable_id)
{
	ScanIterator iterator = ts_scan_iterator_create(CONTINUOUS_AGGS_HYPERTABLE_INVALIDATION_LOG,
													RowExclusiveLock,
													CurrentMemoryContext);

	init_hypertable_invalidation_log_scan_by_hypertable_id(&iterator, raw_hypertable_id);

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));
	}
}

static void
materialization_invalidation_log_delete(int32 materialization_id)
{
	ScanIterator iterator =
		ts_scan_iterator_create(CONTINUOUS_AGGS_MATERIALIZATION_INVALIDATION_LOG,
								RowExclusiveLock,
								CurrentMemoryContext);

	init_materialization_invalidation_log_scan_by_materialization_id(&iterator, materialization_id);

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));
	}
}

static void
continuous_agg_init(ContinuousAgg *cagg, const Form_continuous_agg fd)
{
	Oid nspid = get_namespace_oid(NameStr(fd->user_view_schema), false);
	Hypertable *cagg_ht = ts_hypertable_get_by_id(fd->mat_hypertable_id);
	const Dimension *time_dim;

	Assert(NULL != cagg_ht);
	time_dim = hyperspace_get_open_dimension(cagg_ht->space, 0);
	Assert(NULL != time_dim);
	cagg->partition_type = ts_dimension_get_partition_type(time_dim);
	cagg->relid = get_relname_relid(NameStr(fd->user_view_name), nspid);
	memcpy(&cagg->data, fd, sizeof(cagg->data));

	Assert(OidIsValid(cagg->relid));
	Assert(OidIsValid(cagg->partition_type));
}

TSDLLEXPORT ContinuousAggHypertableStatus
ts_continuous_agg_hypertable_status(int32 hypertable_id)
{
	ScanIterator iterator =
		ts_scan_iterator_create(CONTINUOUS_AGG, AccessShareLock, CurrentMemoryContext);
	ContinuousAggHypertableStatus status = HypertableIsNotContinuousAgg;

	ts_scanner_foreach(&iterator)
	{
		bool should_free;
		HeapTuple tuple = ts_scan_iterator_fetch_heap_tuple(&iterator, false, &should_free);
		FormData_continuous_agg *data = (FormData_continuous_agg *) GETSTRUCT(tuple);

		if (data->raw_hypertable_id == hypertable_id)
			status |= HypertableIsRawTable;
		if (data->mat_hypertable_id == hypertable_id)
			status |= HypertableIsMaterialization;

		if (should_free)
			heap_freetuple(tuple);

		if (status == HypertableIsMaterializationAndRaw)
		{
			ts_scan_iterator_close(&iterator);
			return status;
		}
	}

	return status;
}

TSDLLEXPORT List *
ts_continuous_aggs_find_by_raw_table_id(int32 raw_hypertable_id)
{
	List *continuous_aggs = NIL;
	ScanIterator iterator =
		ts_scan_iterator_create(CONTINUOUS_AGG, AccessShareLock, CurrentMemoryContext);

	init_scan_by_raw_hypertable_id(&iterator, raw_hypertable_id);
	ts_scanner_foreach(&iterator)
	{
		ContinuousAgg *ca;
		bool should_free;
		HeapTuple tuple = ts_scan_iterator_fetch_heap_tuple(&iterator, false, &should_free);
		Form_continuous_agg data = (Form_continuous_agg) GETSTRUCT(tuple);
		MemoryContext oldmctx;

		oldmctx = MemoryContextSwitchTo(ts_scan_iterator_get_result_memory_context(&iterator));
		ca = palloc0(sizeof(*ca));
		continuous_agg_init(ca, data);
		continuous_aggs = lappend(continuous_aggs, ca);
		MemoryContextSwitchTo(oldmctx);

		if (should_free)
			heap_freetuple(tuple);
	}

	return continuous_aggs;
}

/* Find a continuous aggregate by the materialized hypertable id */
ContinuousAgg *
ts_continuous_agg_find_by_mat_hypertable_id(int32 mat_hypertable_id)
{
	ContinuousAgg *ca = NULL;
	ScanIterator iterator =
		ts_scan_iterator_create(CONTINUOUS_AGG, RowExclusiveLock, CurrentMemoryContext);

	init_scan_by_mat_hypertable_id(&iterator, mat_hypertable_id);
	ts_scanner_foreach(&iterator)
	{
		bool should_free;
		HeapTuple tuple = ts_scan_iterator_fetch_heap_tuple(&iterator, false, &should_free);
		Form_continuous_agg form = (Form_continuous_agg) GETSTRUCT(tuple);

		/* Note that this scan can only match at most once, so we assert on
		 * `ca` here. */
		Assert(ca == NULL);
		ca = ts_scan_iterator_alloc_result(&iterator, sizeof(*ca));
		continuous_agg_init(ca, form);

		Assert(ca && ca->data.mat_hypertable_id == mat_hypertable_id);

		if (should_free)
			heap_freetuple(tuple);
	}
	ts_scan_iterator_close(&iterator);
	return ca;
}

static bool
continuous_agg_fill_form_data(const char *schema, const char *name, ContinuousAggViewType type,
							  FormData_continuous_agg *fd)
{
	ScanIterator iterator;
	AttrNumber view_name_attrnum = 0;
	AttrNumber schema_name_attrnum = 0;
	int count = 0;

	Assert(schema);
	Assert(name);

	switch (type)
	{
		case ContinuousAggUserView:
			schema_name_attrnum = Anum_continuous_agg_user_view_schema;
			view_name_attrnum = Anum_continuous_agg_user_view_name;
			break;
		case ContinuousAggPartialView:
			schema_name_attrnum = Anum_continuous_agg_partial_view_schema;
			view_name_attrnum = Anum_continuous_agg_partial_view_name;
			break;
		case ContinuousAggDirectView:
			schema_name_attrnum = Anum_continuous_agg_direct_view_schema;
			view_name_attrnum = Anum_continuous_agg_direct_view_name;
			break;
		case ContinuousAggAnyView:
			break;
	}

	iterator = ts_scan_iterator_create(CONTINUOUS_AGG, AccessShareLock, CurrentMemoryContext);

	if (type != ContinuousAggAnyView)
	{
		ts_scan_iterator_scan_key_init(&iterator,
									   schema_name_attrnum,
									   BTEqualStrategyNumber,
									   F_NAMEEQ,
									   CStringGetDatum(schema));
		ts_scan_iterator_scan_key_init(&iterator,
									   view_name_attrnum,
									   BTEqualStrategyNumber,
									   F_NAMEEQ,
									   CStringGetDatum(name));
	}

	ts_scanner_foreach(&iterator)
	{
		bool should_free;
		HeapTuple tuple = ts_scan_iterator_fetch_heap_tuple(&iterator, false, &should_free);
		FormData_continuous_agg *data = (FormData_continuous_agg *) GETSTRUCT(tuple);
		ContinuousAggViewType vtype = type;

		if (vtype == ContinuousAggAnyView)
			vtype = ts_continuous_agg_view_type(data, schema, name);

		if (vtype != ContinuousAggAnyView)
		{
			memcpy(fd, data, sizeof(*fd));
			count++;
		}

		if (should_free)
			heap_freetuple(tuple);
	}

	Assert(count <= 1);

	return count == 1;
}

ContinuousAgg *
ts_continuous_agg_find_by_view_name(const char *schema, const char *name,
									ContinuousAggViewType type)
{
	FormData_continuous_agg fd;
	ContinuousAgg *ca;

	if (!continuous_agg_fill_form_data(schema, name, type, &fd))
		return NULL;

	ca = palloc0(sizeof(ContinuousAgg));
	continuous_agg_init(ca, &fd);

	return ca;
}

ContinuousAgg *
ts_continuous_agg_find_userview_name(const char *schema, const char *name)
{
	return ts_continuous_agg_find_by_view_name(schema, name, ContinuousAggUserView);
}

/*
 * Find a continuous agg object by the main relid.
 *
 * The relid is the user-facing object ID that represents the continuous
 * aggregate (i.e., the query view's ID).
 */
ContinuousAgg *
ts_continuous_agg_find_by_relid(Oid relid)
{
	const char *relname = get_rel_name(relid);
	const char *schemaname = get_namespace_name(get_rel_namespace(relid));

	if (NULL == relname || NULL == schemaname)
		return NULL;

	return ts_continuous_agg_find_userview_name(schemaname, relname);
}

/*
 * Find a continuous aggregate by range var.
 */
ContinuousAgg *
ts_continuous_agg_find_by_rv(const RangeVar *rv)
{
	Oid relid;
	if (rv == NULL)
		return NULL;
	relid = RangeVarGetRelid(rv, NoLock, true);
	if (!OidIsValid(relid))
		return NULL;
	return ts_continuous_agg_find_by_relid(relid);
}

static ObjectAddress
get_and_lock_rel_by_name(const Name schema, const Name name, LOCKMODE mode)
{
	ObjectAddress addr;
	Oid relid = InvalidOid;
	Oid nspid = get_namespace_oid(NameStr(*schema), true);
	if (OidIsValid(nspid))
	{
		relid = get_relname_relid(NameStr(*name), nspid);
		if (OidIsValid(relid))
			LockRelationOid(relid, mode);
	}
	ObjectAddressSet(addr, RelationRelationId, relid);
	return addr;
}

static ObjectAddress
get_and_lock_rel_by_hypertable_id(int32 hypertable_id, LOCKMODE mode)
{
	ObjectAddress addr;
	Oid relid = ts_hypertable_id_to_relid(hypertable_id);
	if (OidIsValid(relid))
		LockRelationOid(relid, mode);
	ObjectAddressSet(addr, RelationRelationId, relid);
	return addr;
}

/*
 * Drops continuous aggs and all related objects.
 *
 * This function is intended to be run by event trigger during CASCADE,
 * which implies that most of the dependent objects potentially could be
 * dropped including associated schema.
 *
 * These objects are:
 *
 * - user view itself
 * - continuous agg catalog entry
 * - partial view
 * - materialization hypertable
 * - trigger on the raw hypertable (hypertable specified in the user view)
 * - copy of the user view query (AKA the direct view)
 *
 * NOTE: The order in which the objects are dropped should be EXACTLY the
 * same as in materialize.c
 *
 * drop_user_view indicates whether to drop the user view.
 *                (should be false if called as part of the drop-user-view callback)
 */
static void
drop_continuous_agg(FormData_continuous_agg *cadata, bool drop_user_view)
{
	Catalog *catalog;
	ScanIterator iterator;
	ObjectAddress user_view = { 0 };
	ObjectAddress partial_view = { 0 };
	ObjectAddress direct_view = { 0 };
	ObjectAddress raw_hypertable_trig = { 0 };
	ObjectAddress raw_hypertable = { 0 };
	ObjectAddress mat_hypertable = { 0 };
	bool raw_hypertable_has_other_caggs;

	/* Delete the job before taking locks as it kills long-running jobs
	 * which we would otherwise wait on */
	List *jobs = ts_bgw_job_find_by_hypertable_id(cadata->mat_hypertable_id);
	ListCell *lc;

	foreach (lc, jobs)
	{
		BgwJob *job = lfirst(lc);
		ts_bgw_job_delete_by_id(job->fd.id);
	}

	/*
	 * Lock objects.
	 *
	 * Following objects might be already dropped in case of CASCADE
	 * drop including the associated schema object.
	 *
	 * NOTE: the lock order matters, see tsl/src/materialization.c.
	 * Perform all locking upfront.
	 *
	 * AccessExclusiveLock is needed to drop triggers and also prevent
	 * concurrent DML commands.
	 */
	if (drop_user_view)
		user_view = get_and_lock_rel_by_name(&cadata->user_view_schema,
											 &cadata->user_view_name,
											 AccessExclusiveLock);
	raw_hypertable =
		get_and_lock_rel_by_hypertable_id(cadata->raw_hypertable_id, AccessExclusiveLock);
	mat_hypertable =
		get_and_lock_rel_by_hypertable_id(cadata->mat_hypertable_id, AccessExclusiveLock);

	/* Lock catalogs */
	catalog = ts_catalog_get();
	LockRelationOid(catalog_get_table_id(catalog, BGW_JOB), RowExclusiveLock);
	LockRelationOid(catalog_get_table_id(catalog, CONTINUOUS_AGG), RowExclusiveLock);

	raw_hypertable_has_other_caggs =
		OidIsValid(raw_hypertable.objectId) &&
		number_of_continuous_aggs_attached(cadata->raw_hypertable_id) > 1;

	if (!raw_hypertable_has_other_caggs)
	{
		LockRelationOid(catalog_get_table_id(catalog, CONTINUOUS_AGGS_HYPERTABLE_INVALIDATION_LOG),
						RowExclusiveLock);
		LockRelationOid(catalog_get_table_id(catalog, CONTINUOUS_AGGS_INVALIDATION_THRESHOLD),
						RowExclusiveLock);

		/* The trigger will be dropped if the hypertable still exists and no other
		 * caggs attached. */
		if (OidIsValid(raw_hypertable.objectId))
		{
			ObjectAddressSet(raw_hypertable_trig,
							 TriggerRelationId,
							 get_trigger_oid(raw_hypertable.objectId,
											 CAGGINVAL_TRIGGER_NAME,
											 false));

			/* Raw hypertable is locked above */
			LockRelationOid(raw_hypertable_trig.objectId, AccessExclusiveLock);
		}
	}

	/*
	 * Following objects might be already dropped in case of CASCADE
	 * drop including the associated schema object.
	 */
	partial_view = get_and_lock_rel_by_name(&cadata->partial_view_schema,
											&cadata->partial_view_name,
											AccessExclusiveLock);

	direct_view = get_and_lock_rel_by_name(&cadata->direct_view_schema,
										   &cadata->direct_view_name,
										   AccessExclusiveLock);

	/* Delete catalog entry */
	iterator = ts_scan_iterator_create(CONTINUOUS_AGG, RowExclusiveLock, CurrentMemoryContext);
	init_scan_by_mat_hypertable_id(&iterator, cadata->mat_hypertable_id);

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		bool should_free;
		HeapTuple tuple = ts_scan_iterator_fetch_heap_tuple(&iterator, false, &should_free);
		Form_continuous_agg form = (Form_continuous_agg) GETSTRUCT(tuple);

		ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));

		/* Delete all related rows */
		if (!raw_hypertable_has_other_caggs)
		{
			hypertable_invalidation_log_delete(form->raw_hypertable_id);
			invalidation_threshold_delete(form->raw_hypertable_id);
		}

		materialization_invalidation_log_delete(form->mat_hypertable_id);

		if (should_free)
			heap_freetuple(tuple);
	}

	/* Perform actual deletions now */
	if (OidIsValid(user_view.objectId))
		performDeletion(&user_view, DROP_RESTRICT, 0);

	if (OidIsValid(raw_hypertable_trig.objectId))
		ts_hypertable_drop_trigger(raw_hypertable.objectId, CAGGINVAL_TRIGGER_NAME);

	if (OidIsValid(mat_hypertable.objectId))
	{
		performDeletion(&mat_hypertable, DROP_CASCADE, 0);
		ts_hypertable_delete_by_id(cadata->mat_hypertable_id);
	}

	if (OidIsValid(partial_view.objectId))
		performDeletion(&partial_view, DROP_RESTRICT, 0);

	if (OidIsValid(direct_view.objectId))
		performDeletion(&direct_view, DROP_RESTRICT, 0);
}

/*
 * This is a called when a hypertable gets dropped.
 *
 * If the hypertable is a raw hypertable for a continuous agg,
 * drop the continuous agg.
 *
 * If the hypertable is a materialization hypertable, error out
 * and force the user to drop the continuous agg instead.
 */
void
ts_continuous_agg_drop_hypertable_callback(int32 hypertable_id)
{
	ScanIterator iterator =
		ts_scan_iterator_create(CONTINUOUS_AGG, AccessShareLock, CurrentMemoryContext);

	ts_scanner_foreach(&iterator)
	{
		bool should_free;
		HeapTuple tuple = ts_scan_iterator_fetch_heap_tuple(&iterator, false, &should_free);
		FormData_continuous_agg *data = (FormData_continuous_agg *) GETSTRUCT(tuple);

		if (data->raw_hypertable_id == hypertable_id)
			drop_continuous_agg(data, true);

		if (data->mat_hypertable_id == hypertable_id)
			ereport(ERROR,
					(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
					 errmsg("cannot drop the materialized table because it is required by a "
							"continuous aggregate")));

		if (should_free)
			heap_freetuple(tuple);
	}
}

/* Block dropping the partial and direct view if the continuous aggregate still exists */
static void
drop_internal_view(const FormData_continuous_agg *fd)
{
	ScanIterator iterator =
		ts_scan_iterator_create(CONTINUOUS_AGG, AccessShareLock, CurrentMemoryContext);
	int count = 0;
	init_scan_by_mat_hypertable_id(&iterator, fd->mat_hypertable_id);
	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));
		count++;
	}
	if (count > 0)
		ereport(ERROR,
				(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
				 errmsg(
					 "cannot drop the partial/direct view because it is required by a continuous "
					 "aggregate")));
}

/* This gets called when a view gets dropped. */
static void
continuous_agg_drop_view_callback(FormData_continuous_agg *fd, const char *schema, const char *name)
{
	ContinuousAggViewType vtyp;
	vtyp = ts_continuous_agg_view_type(fd, schema, name);
	switch (vtyp)
	{
		case ContinuousAggUserView:
			drop_continuous_agg(fd, false /* The user view has already been dropped */);
			break;
		case ContinuousAggPartialView:
		case ContinuousAggDirectView:
			drop_internal_view(fd);
			break;
		default:
			elog(ERROR, "unknown continuous aggregate view type");
	}
}

bool
ts_continuous_agg_drop(const char *view_schema, const char *view_name)
{
	FormData_continuous_agg fd;
	bool found = continuous_agg_fill_form_data(view_schema, view_name, ContinuousAggAnyView, &fd);

	if (found)
		continuous_agg_drop_view_callback(&fd, view_schema, view_name);

	return found;
}

static inline bool
ts_continuous_agg_is_user_view_schema(FormData_continuous_agg *data, const char *schema)
{
	return CHECK_NAME_MATCH(&data->user_view_schema, schema);
}

static inline bool
ts_continuous_agg_is_partial_view_schema(FormData_continuous_agg *data, const char *schema)
{
	return CHECK_NAME_MATCH(&data->partial_view_schema, schema);
}

static inline bool
ts_continuous_agg_is_direct_view_schema(FormData_continuous_agg *data, const char *schema)
{
	return CHECK_NAME_MATCH(&data->direct_view_schema, schema);
}

ContinuousAggViewType
ts_continuous_agg_view_type(FormData_continuous_agg *data, const char *schema, const char *name)
{
	if (CHECK_NAME_MATCH(&data->user_view_schema, schema) &&
		CHECK_NAME_MATCH(&data->user_view_name, name))
		return ContinuousAggUserView;
	else if (CHECK_NAME_MATCH(&data->partial_view_schema, schema) &&
			 CHECK_NAME_MATCH(&data->partial_view_name, name))
		return ContinuousAggPartialView;
	else if (CHECK_NAME_MATCH(&data->direct_view_schema, schema) &&
			 CHECK_NAME_MATCH(&data->direct_view_name, name))
		return ContinuousAggDirectView;
	else
		return ContinuousAggAnyView;
}

static FormData_continuous_agg *
ensure_new_tuple(HeapTuple old_tuple, HeapTuple *new_tuple)
{
	if (*new_tuple == NULL)
		*new_tuple = heap_copytuple(old_tuple);

	return (FormData_continuous_agg *) GETSTRUCT(*new_tuple);
}

void
ts_continuous_agg_rename_schema_name(char *old_schema, char *new_schema)
{
	ScanIterator iterator =
		ts_scan_iterator_create(CONTINUOUS_AGG, RowExclusiveLock, CurrentMemoryContext);

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *tinfo = ts_scan_iterator_tuple_info(&iterator);
		bool should_free;
		HeapTuple tuple = ts_scan_iterator_fetch_heap_tuple(&iterator, false, &should_free);
		FormData_continuous_agg *data = (FormData_continuous_agg *) GETSTRUCT(tuple);
		HeapTuple new_tuple = NULL;

		if (ts_continuous_agg_is_user_view_schema(data, old_schema))
		{
			FormData_continuous_agg *new_data = ensure_new_tuple(tuple, &new_tuple);
			namestrcpy(&new_data->user_view_schema, new_schema);
		}

		if (ts_continuous_agg_is_partial_view_schema(data, old_schema))
		{
			FormData_continuous_agg *new_data = ensure_new_tuple(tuple, &new_tuple);
			namestrcpy(&new_data->partial_view_schema, new_schema);
		}

		if (ts_continuous_agg_is_direct_view_schema(data, old_schema))
		{
			FormData_continuous_agg *new_data = ensure_new_tuple(tuple, &new_tuple);
			namestrcpy(&new_data->direct_view_schema, new_schema);
		}

		if (new_tuple != NULL)
		{
			ts_catalog_update(tinfo->scanrel, new_tuple);
			heap_freetuple(new_tuple);
		}

		if (should_free)
			heap_freetuple(tuple);
	}
	return;
}

extern void
ts_continuous_agg_rename_view(const char *old_schema, const char *name, const char *new_schema,
							  const char *new_name, ObjectType *object_type)
{
	ScanIterator iterator =
		ts_scan_iterator_create(CONTINUOUS_AGG, RowExclusiveLock, CurrentMemoryContext);

	Assert(object_type);

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *tinfo = ts_scan_iterator_tuple_info(&iterator);
		bool should_free;
		HeapTuple tuple = ts_scan_iterator_fetch_heap_tuple(&iterator, false, &should_free);
		FormData_continuous_agg *data = (FormData_continuous_agg *) GETSTRUCT(tuple);
		HeapTuple new_tuple = NULL;
		ContinuousAggViewType vtyp = ts_continuous_agg_view_type(data, old_schema, name);

		switch (vtyp)
		{
			case ContinuousAggUserView:
			{
				FormData_continuous_agg *new_data;

				if (*object_type == OBJECT_VIEW)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot alter continuous aggregate using ALTER VIEW"),
							 errhint(
								 "Use ALTER MATERIALIZED VIEW to alter a continuous aggregate.")));

				Assert(*object_type == OBJECT_MATVIEW);
				*object_type = OBJECT_VIEW;

				new_data = ensure_new_tuple(tuple, &new_tuple);
				namestrcpy(&new_data->user_view_schema, new_schema);
				namestrcpy(&new_data->user_view_name, new_name);
				break;
			}
			case ContinuousAggPartialView:
			{
				FormData_continuous_agg *new_data = ensure_new_tuple(tuple, &new_tuple);
				namestrcpy(&new_data->partial_view_schema, new_schema);
				namestrcpy(&new_data->partial_view_name, new_name);
				break;
			}
			case ContinuousAggDirectView:
			{
				FormData_continuous_agg *new_data = ensure_new_tuple(tuple, &new_tuple);
				namestrcpy(&new_data->direct_view_schema, new_schema);
				namestrcpy(&new_data->direct_view_name, new_name);
				break;
			}
			default:
				break;
		}

		if (new_tuple != NULL)
		{
			ts_catalog_update(tinfo->scanrel, new_tuple);
			heap_freetuple(new_tuple);
		}

		if (should_free)
			heap_freetuple(tuple);
	}
	return;
}

TSDLLEXPORT int32
ts_number_of_continuous_aggs()
{
	int32 count = 0;
	ScanIterator iterator =
		ts_scan_iterator_create(CONTINUOUS_AGG, AccessShareLock, CurrentMemoryContext);
	ts_scanner_foreach(&iterator) { count++; }

	return count;
}

Oid
ts_continuous_agg_get_user_view_oid(ContinuousAgg *agg)
{
	Oid view_relid =
		get_relname_relid(NameStr(agg->data.user_view_name),
						  get_namespace_oid(NameStr(agg->data.user_view_schema), false));
	if (!OidIsValid(view_relid))
		elog(ERROR, "could not find user view for continuous agg");
	return view_relid;
}

static int32
find_raw_hypertable_for_materialization(int32 mat_hypertable_id)
{
	short count = 0;
	int32 htid = INVALID_HYPERTABLE_ID;
	ScanIterator iterator =
		ts_scan_iterator_create(CONTINUOUS_AGG, RowExclusiveLock, CurrentMemoryContext);

	init_scan_by_mat_hypertable_id(&iterator, mat_hypertable_id);
	ts_scanner_foreach(&iterator)
	{
		bool isnull;
		Datum datum = slot_getattr(ts_scan_iterator_slot(&iterator),
								   Anum_continuous_agg_raw_hypertable_id,
								   &isnull);

		Assert(!isnull);
		htid = DatumGetInt32(datum);
		count++;
	}
	Assert(count <= 1);
	ts_scan_iterator_close(&iterator);
	return htid;
}

/* Continuous aggregate materialization hypertables inherit integer_now func
 * from the raw hypertable (unless it was explicitly reset for cont. aggregate.
 * Walk the materialization hypertable ->raw hypertable tree till
 * we find a hypertable that has integer_now_func set.
 */
TSDLLEXPORT const Dimension *
ts_continuous_agg_find_integer_now_func_by_materialization_id(int32 mat_htid)
{
	int32 raw_htid = mat_htid;
	const Dimension *par_dim = NULL;
	while (raw_htid != INVALID_HYPERTABLE_ID)
	{
		Hypertable *raw_ht = ts_hypertable_get_by_id(raw_htid);
		const Dimension *open_dim = hyperspace_get_open_dimension(raw_ht->space, 0);
		if (strlen(NameStr(open_dim->fd.integer_now_func)) != 0 &&
			strlen(NameStr(open_dim->fd.integer_now_func_schema)) != 0)
		{
			par_dim = open_dim;
			break;
		}
		mat_htid = raw_htid;
		raw_htid = find_raw_hypertable_for_materialization(mat_htid);
	}
	return par_dim;
}

typedef struct Watermark
{
	int32 hyper_id;
	MemoryContext mctx;
	MemoryContextCallback cb;
	CommandId cid;
	int64 value;
} Watermark;

/* Globally cache the watermark for better performance (by avoiding repeated
 * max bucket calculations). The watermark will be reset at the end of the
 * transaction, when the watermark function's input argument (materialized
 * hypertable ID) changes, or when a new command is executed. One could
 * potentially create a hashtable of watermarks keyed on materialized
 * hypertable ID, but this is left as a future optimization since it doesn't
 * seem to be common case that multiple continuous aggregates exist in the
 * same query. Besides, the planner can constify calls to the watermark
 * function during planning since the function is STABLE. Therefore, this is
 * only a fallback if the planner needs to constify it many times (e.g., if
 * used as an index condition on many chunks).
 */
static Watermark *watermark = NULL;

/*
 * Callback handler to reset the watermark after the transaction ends. This is
 * triggered by the deletion of the associated memory context.
 */
static void
reset_watermark(void *arg)
{
	watermark = NULL;
}

/*
 * Watermark is valid for the duration of one command execution on the same
 * materialized hypertable.
 */
static bool
watermark_valid(const Watermark *w, int32 hyper_id)
{
	return w != NULL && w->hyper_id == hyper_id && w->cid == GetCurrentCommandId(false);
}

static Watermark *
watermark_create(const ContinuousAgg *cagg, MemoryContext top_mctx)
{
	Hypertable *ht;
	const Dimension *dim;
	Datum maxdat;
	bool max_isnull;
	Oid timetype;
	Watermark *w;
	MemoryContext mctx =
		AllocSetContextCreate(top_mctx, "Watermark function", ALLOCSET_DEFAULT_SIZES);

	w = MemoryContextAllocZero(mctx, sizeof(Watermark));
	w->mctx = mctx;
	w->hyper_id = cagg->data.mat_hypertable_id;
	w->cid = GetCurrentCommandId(false);
	w->cb.func = reset_watermark;
	MemoryContextRegisterResetCallback(mctx, &w->cb);

	ht = ts_hypertable_get_by_id(cagg->data.mat_hypertable_id);
	Assert(NULL != ht);
	dim = hyperspace_get_open_dimension(ht->space, 0);
	timetype = ts_dimension_get_partition_type(dim);
	maxdat = ts_hypertable_get_open_dim_max_value(ht, 0, &max_isnull);

	if (!max_isnull)
	{
		int64 value;
		int64 bucket_width = ts_continuous_agg_bucket_width(cagg);

		/* The materialized hypertable is already bucketed, which means the
		 * max is the start of the last bucket. Add one bucket to move to the
		 * point where the materialized data ends. */
		value = ts_time_value_to_internal(maxdat, timetype);
		w->value = ts_time_saturating_add(value, bucket_width, timetype);
	}
	else
	{
		/* Nothing materialized, so return min */
		w->value = ts_time_get_min(timetype);
	}

	return w;
}

TS_FUNCTION_INFO_V1(ts_continuous_agg_watermark);

/*
 * Get the watermark for a real-time aggregation query on a continuous
 * aggregate.
 *
 * The watermark determines where the materialization ends for a continuous
 * aggregate. It is used by real-time aggregation as the threshold between the
 * materialized data and real-time data in the UNION query.
 *
 * The watermark is defined as the end of the last (highest) bucket in the
 * materialized hypertable of a continuous aggregate.
 *
 * The materialized hypertable ID is given as input argument.
 */
Datum
ts_continuous_agg_watermark(PG_FUNCTION_ARGS)
{
	const int32 hyper_id = PG_GETARG_INT32(0);
	ContinuousAgg *cagg;
	AclResult aclresult;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("materialized hypertable cannot be NULL")));

	if (watermark != NULL)
	{
		if (watermark_valid(watermark, hyper_id))
			PG_RETURN_INT64(watermark->value);

		MemoryContextDelete(watermark->mctx);
	}

	cagg = ts_continuous_agg_find_by_mat_hypertable_id(hyper_id);

	if (NULL == cagg)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid materialized hypertable ID: %d", hyper_id)));

	/* Preemptive permission check to ensure the function complains about lack
	 * of permissions on the cagg rather than the materialized hypertable */
	aclresult = pg_class_aclcheck(cagg->relid, GetUserId(), ACL_SELECT);
	aclcheck_error(aclresult, OBJECT_MATVIEW, get_rel_name(cagg->relid));
	watermark = watermark_create(cagg, TopTransactionContext);

	PG_RETURN_INT64(watermark->value);
}

/*
 * Determines bucket width for given continuous aggregate.
 *
 * Currently all buckets are fixed in size but this is going to change in the
 * future. Any code that needs to know bucket width has to determine it using
 * this procedure instead of accessing any ContinuousAgg fields directly.
 */
int64
ts_continuous_agg_bucket_width(const ContinuousAgg *agg)
{
	return agg->data.bucket_width;
}

/*
 * Determines maximum possible bucket width for given continuous aggregate.
 *
 * E.g. for monthly continuous aggreagtes this procedure will return 31 days.
 * This is not true for ts_continuous_agg_bucket_width which may use additional
 * arguments to determine the width of a concrete bucket.
 */
int64
ts_continuous_agg_max_bucket_width(const ContinuousAgg *agg)
{
	return agg->data.bucket_width;
}
