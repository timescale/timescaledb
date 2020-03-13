/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_COMPAT_H
#define TIMESCALEDB_COMPAT_H

#include <commands/trigger.h>
#include <postgres.h>
#include <pgstat.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <executor/executor.h>
#include <executor/tuptable.h>
#include <nodes/execnodes.h>
#include <nodes/nodes.h>

#include "export.h"
#include "import/planner.h"

#define is_supported_pg_version_96(version) ((version >= 90603) && (version < 100000))
#define is_supported_pg_version_10(version) ((version >= 100002) && (version < 110000))
#define is_supported_pg_version_11(version) ((version >= 110000) && (version < 120000))
#define is_supported_pg_version_12(version) ((version >= 120000) && (version < 130000))

#define is_supported_pg_version(version)                                                           \
	(is_supported_pg_version_96(version) || is_supported_pg_version_10(version) ||                 \
	 is_supported_pg_version_11(version) || is_supported_pg_version_12(version))

#define PG96 is_supported_pg_version_96(PG_VERSION_NUM)
#define PG10 is_supported_pg_version_10(PG_VERSION_NUM)
#define PG11 is_supported_pg_version_11(PG_VERSION_NUM)
#define PG12 is_supported_pg_version_12(PG_VERSION_NUM)

#define PG10_LT PG96
#define PG10_GE !(PG10_LT)

#define PG11_LT (PG96 || PG10)
#define PG11_GE !(PG11_LT)

#define PG12_LT (PG96 || PG10 || PG11)
#define PG12_GE !(PG12_LT)

#if !(is_supported_pg_version(PG_VERSION_NUM))
#error "Unsupported PostgreSQL version"
#endif

/*
 * The following are compatibility functions for different versions of
 * PostgreSQL. Each compatibility function (or group) has its own logic for
 * which versions get different behavior and is separated from others by a
 * comment with its name and any clarifying notes about supported behavior. Each
 * compatibility define is separated out by function so that it is easier to see
 * what changed about its behavior, and at what version, but closely related
 * functions that changed at the same time may be grouped together into a single
 * block. Compatibility functions are organized in alphabetical order.
 *
 * Wherever reasonable, we try to achieve forwards compatibility so that we can
 * take advantage of features added in newer PG versions. This avoids some
 * future tech debt, though may not always be possible.
 *
 * We append "compat" to the name of the function or define if we change the behavior
 * of something that existed in a previous version. If we are merely backpatching
 * behavior from a later version to an earlier version and not changing the
 * behavior of the new version we simply adopt the new version's name.
 */

/*
 * adjust_appendrel_attrs
 *
 * PG11 changed the signature of adjust_appendrel_attrs to eventually allow for
 * partitionwise joins. (See:
 * https://github.com/postgres/postgres/commit/480f1f4329f1bf8bfbbcda8ed233851e1b110ad4).
 * We adopt backwards compatibility for our compat, which will be used in place
 * of current call, however, we may wish to use the new version in code that
 * will only be activated in versions >=11 when we implement partition-wise
 * joins.
 */
#if PG11_LT
#define adjust_appendrel_attrs_compat adjust_appendrel_attrs
#else
#define adjust_appendrel_attrs_compat(root, node, appinfo)                                         \
	adjust_appendrel_attrs(root, node, 1, &appinfo)
#endif

/*
 * BackgroundWorkerInitializeConnection
 * BackgroundWorkerInitializeConnectionByOid
 *
 * PG11 introduced flags to BackgroundWorker connection functions. PG96 & 10
 * interface kept for backwards compatibility.
 */
#if PG11_LT
#define BackgroundWorkerInitializeConnectionByOidCompat(dboid, useroid)                            \
	BackgroundWorkerInitializeConnectionByOid(dboid, useroid)
#define BackgroundWorkerInitializeConnectionCompat(dbname, username)                               \
	BackgroundWorkerInitializeConnection(dbname, username)
#else
#define BGWORKER_NO_FLAGS 0

#define BackgroundWorkerInitializeConnectionByOidCompat(dboid, useroid)                            \
	BackgroundWorkerInitializeConnectionByOid(dboid, useroid, BGWORKER_NO_FLAGS)
#define BackgroundWorkerInitializeConnectionCompat(dbname, username)                               \
	BackgroundWorkerInitializeConnection(dbname, username, BGWORKER_NO_FLAGS)
#endif

/* CatalogTuple functions not implemented until pg10 */
#if PG96
#define CatalogTupleInsert(relation, tuple)                                                        \
	do                                                                                             \
	{                                                                                              \
		simple_heap_insert(relation, tuple);                                                       \
		CatalogUpdateIndexes(relation, tuple);                                                     \
	} while (0);

#define CatalogTupleUpdate(relation, tid, tuple)                                                   \
	do                                                                                             \
	{                                                                                              \
		simple_heap_update(relation, tid, tuple);                                                  \
		CatalogUpdateIndexes(relation, tuple);                                                     \
	} while (0);

#define CatalogTupleDelete(relation, tid) simple_heap_delete(relation, tid);

#endif

/* CheckValidResultRel */
#if PG96
#define CheckValidResultRelCompat(relinfo, operation)                                              \
	CheckValidResultRel((relinfo)->ri_RelationDesc, operation)
#else
#define CheckValidResultRelCompat(relinfo, operation) CheckValidResultRel(relinfo, operation)
#endif

/* ConstraintRelidTypidNameIndexId
 *
 * Index names were changed in PG11 to fully enforce uniqueness:
 * https://github.com/postgres/postgres/commit/17b7c302b5fc92bd0241c452599019e18df074dc
 * we use the PG11 version as it is more descriptive.
 */
#if PG11_LT
#define ConstraintRelidTypidNameIndexId ConstraintRelidIndexId
#endif

/*
 * CreateTrigger
 *
 * PG11 allows for-each row triggers on declaratively partitioned tables:
 * https://github.com/postgres/postgres/commit/86f575948c773b0ec5b0f27066e37dd93a7f0a96
 * It adds multiple fields to this function call for dealing with it. As we deal
 * with it separately, we instead maintain backwards compatibility for the old
 * interface and continue to manage as before.
 */
#if PG11_LT
#define CreateTriggerCompat CreateTrigger
#else
#define CreateTriggerCompat(stmt,                                                                  \
							queryString,                                                           \
							relOid,                                                                \
							refRelOid,                                                             \
							constraintOid,                                                         \
							indexOid,                                                              \
							isInternal)                                                            \
	CreateTrigger(stmt,                                                                            \
				  queryString,                                                                     \
				  relOid,                                                                          \
				  refRelOid,                                                                       \
				  constraintOid,                                                                   \
				  indexOid,                                                                        \
				  InvalidOid,                                                                      \
				  InvalidOid,                                                                      \
				  NULL,                                                                            \
				  isInternal,                                                                      \
				  false)
#endif

/*
 * DefineIndex
 *
 * PG10 introduced check_not_in_use boolean between check_rights & skip_build
 * PG11 introduced parentIndexId and parentConstraintId oids between
 * indexRelationId and is_alter_table they are used for declaratively
 * partitioned tables, InvalidOid otherwise.
 * The PG96 interface is used for compatibility.
 */
#if PG96
#define DefineIndexCompat DefineIndex
#elif PG10
#define DefineIndexCompat(relationId,                                                              \
						  stmt,                                                                    \
						  indexRelationId,                                                         \
						  is_alter_table,                                                          \
						  check_rights,                                                            \
						  skip_build,                                                              \
						  quiet)                                                                   \
	DefineIndex(relationId,                                                                        \
				stmt,                                                                              \
				indexRelationId,                                                                   \
				is_alter_table,                                                                    \
				check_rights,                                                                      \
				false,                                                                             \
				skip_build,                                                                        \
				quiet)
#else
#define DefineIndexCompat(relationId,                                                              \
						  stmt,                                                                    \
						  indexRelationId,                                                         \
						  is_alter_table,                                                          \
						  check_rights,                                                            \
						  skip_build,                                                              \
						  quiet)                                                                   \
	DefineIndex(relationId,                                                                        \
				stmt,                                                                              \
				indexRelationId,                                                                   \
				InvalidOid,                                                                        \
				InvalidOid,                                                                        \
				is_alter_table,                                                                    \
				check_rights,                                                                      \
				false,                                                                             \
				skip_build,                                                                        \
				quiet)
#endif

#if PG96
#define DefineRelationCompat(stmt, relkind, ownerid, typaddress, queryString)                      \
	DefineRelation(stmt, relkind, ownerid, typaddress)
#else
#define DefineRelationCompat(stmt, relkind, ownerid, typaddress, queryString)                      \
	DefineRelation(stmt, relkind, ownerid, typaddress, queryString)
#endif

#if PG12_GE
#define ExecInsertIndexTuplesCompat(slot, estate, no_dup_err, spec_conflict, arbiter_indexes)      \
	ExecInsertIndexTuples(slot, estate, no_dup_err, spec_conflict, arbiter_indexes);
#else
#define ExecInsertIndexTuplesCompat(slot, estate, no_dup_err, spec_conflict, arbiter_indexes)      \
	ExecInsertIndexTuples(slot,                                                                    \
						  &((slot)->tts_tuple->t_self),                                            \
						  estate,                                                                  \
						  no_dup_err,                                                              \
						  spec_conflict,                                                           \
						  arbiter_indexes)
#endif

/* ExecARInsertTriggers */
#if PG96
#define ExecARInsertTriggersCompat(estate, relinfo, slot, recheck_indexes, transition_capture)     \
	do                                                                                             \
	{                                                                                              \
		bool should_free;                                                                          \
		HeapTuple tuple = ExecFetchSlotHeapTuple(slot, true, &should_free);                        \
		ExecARInsertTriggers(estate, relinfo, tuple, recheck_indexes);                             \
		if (should_free)                                                                           \
			heap_freetuple(tuple);                                                                 \
	} while (0);
#elif PG12_LT
#define ExecARInsertTriggersCompat(estate, relinfo, slot, recheck_indexes, transition_capture)     \
	do                                                                                             \
	{                                                                                              \
		bool should_free;                                                                          \
		HeapTuple tuple = ExecFetchSlotHeapTuple(slot, true, &should_free);                        \
		ExecARInsertTriggers(estate, relinfo, tuple, recheck_indexes, transition_capture);         \
		if (should_free)                                                                           \
			heap_freetuple(tuple);                                                                 \
	} while (0);
#else
#define ExecARInsertTriggersCompat(estate,                                                         \
								   result_rel_info,                                                \
								   tuple,                                                          \
								   recheck_indexes,                                                \
								   transition_capture)                                             \
	ExecARInsertTriggers(estate, result_rel_info, tuple, recheck_indexes, transition_capture)
#endif

/* ExecASInsertTriggers */
#if PG96
#define ExecASInsertTriggersCompat(estate, result_rel_info)                                        \
	ExecASInsertTriggers(estate, result_rel_info)
#else
#define ExecASInsertTriggersCompat(estate, result_rel_info)                                        \
	ExecASInsertTriggers(estate, result_rel_info, NULL)
#endif

/* execute_attr_map_tuple */
#if PG12_LT
#define execute_attr_map_tuple do_convert_tuple
#endif

/* ExecBuildProjectionInfo */
#if PG96
#define ExecBuildProjectionInfoCompat(tl, exprContext, slot, parent, inputdesc)                    \
	ExecBuildProjectionInfo((List *) ExecInitExpr((Expr *) tl, NULL), exprContext, slot, inputdesc)
#else
#define ExecBuildProjectionInfoCompat(tl, exprContext, slot, parent, inputdesc)                    \
	ExecBuildProjectionInfo(tl, exprContext, slot, parent, inputdesc)
#endif

#if PG12_LT
#define TM_Result HTSU_Result

#define TM_Ok HeapTupleMayBeUpdated
#define TM_SelfModified HeapTupleSelfUpdated
#define TM_Updated HeapTupleUpdated
#define TM_BeingModified HeapTupleBeingUpdated
#define TM_WouldBlock HeapTupleWouldBlock
#define TM_Invisible HeapTupleInvisible

#define TM_FailureData HeapUpdateFailureData
#endif

#if PG12_LT

#define TupleTableSlotOps void
#define TTSOpsVirtualP NULL
#define TTSOpsHeapTupleP NULL
#define TTSOpsMinimalTupleP NULL
#define TTSOpsBufferHeapTupleP NULL

#else

#define TTSOpsVirtualP (&TTSOpsVirtual)
#define TTSOpsHeapTupleP (&TTSOpsHeapTuple)
#define TTSOpsMinimalTupleP (&TTSOpsMinimalTuple)
#define TTSOpsBufferHeapTupleP (&TTSOpsBufferHeapTuple)

#endif

/*
 * ExecInitExtraTupleSlot & MakeTupleTableSlot
 *
 * PG11 introduced the ability to pass in a tupledesc and avoid having to
 * separately run ExecSetSlotDescriptor. Additionally, it added the ability to have a
 * fixed tupleDesc when making a TupleTableSlot, which can be useful during
 * execution for JITted operations. (See:
 * https://github.com/postgres/postgres/commit/ad7dbee368a7cd9e595d2a957be784326b08c943).
 * We adopt the PG11 conventions so that we can take advantage of JITing more easily in the future.
 */
#if PG11_LT

static inline TupleTableSlot *
ExecInitExtraTupleSlotCompat(EState *estate, TupleDesc tupdesc, void *tts_ops)
{
	TupleTableSlot *myslot = ExecInitExtraTupleSlot(estate);

	if (tupdesc != NULL)
		ExecSetSlotDescriptor(myslot, tupdesc);

	return myslot;
}

#define MakeSingleTupleTableSlotCompat(tupledesc, tts_ops) MakeSingleTupleTableSlot(tupledesc)

/*
 * ExecSetTupleBound is only available starting with PG11 so we map to a backported version
 * for PG9.6 and PG10
 */
#define ExecSetTupleBound(tuples_needed, child_node) ts_ExecSetTupleBound(tuples_needed, child_node)

static inline TupleTableSlot *
MakeTupleTableSlotCompat(TupleDesc tupdesc, void *tts_ops)
{
	TupleTableSlot *myslot = MakeTupleTableSlot();

	if (tupdesc != NULL)
		ExecSetSlotDescriptor(myslot, tupdesc);

	return myslot;
}
#elif PG11

#define ExecInitExtraTupleSlotCompat(estate, tupledesc, tts_ops)                                   \
	ExecInitExtraTupleSlot(estate, tupledesc)
#define MakeTupleTableSlotCompat(tupdesc, tts_ops) MakeTupleTableSlot(tupdesc)
#define MakeSingleTupleTableSlotCompat(tupdesc, tts_ops) MakeSingleTupleTableSlot(tupdesc)

#else /* PG12_GE */

#define ExecInitExtraTupleSlotCompat(estate, tupdesc, tts_ops)                                     \
	ExecInitExtraTupleSlot(estate, tupdesc, tts_ops)
#define MakeTupleTableSlotCompat(tupdesc, tts_ops) MakeTupleTableSlot(tupdesc, tts_ops)
#define MakeSingleTupleTableSlotCompat(tupdesc, tts_ops) MakeSingleTupleTableSlot(tupdesc, tts_ops)

#endif

/* fmgr
 * In a9c35cf postgres changed how it calls SQL functions so that the number of
 * argument-slots allocated is chosen dynamically, instead of being fixed. This
 * change was ABI-breaking, so we cannot backport this optimization, however,
 * we do backport the interface, so that all our code will be compatible with
 * new versions.
 */
#if PG12_LT

/* unlike the pg12 version, this is just a wrapper for FunctionCallInfoData */
#define LOCAL_FCINFO(name, nargs)                                                                  \
	union                                                                                          \
	{                                                                                              \
		FunctionCallInfoData fcinfo;                                                               \
	} name##data;                                                                                  \
	FunctionCallInfo name = &name##data.fcinfo

/* convenience macro to allocate FunctionCallInfoData on the heap */
#define HEAP_FCINFO(nargs) palloc(sizeof(FunctionCallInfoData))

/* getting arguments has a different API, so these macros unify the versions */
#define FC_ARG(fcinfo, n) ((fcinfo)->arg[(n)])
#define FC_NULL(fcinfo, n) ((fcinfo)->argnull[(n)])

#else

/* convenience macro to allocate FunctionCallInfoData on the heap */
#define HEAP_FCINFO(nargs) palloc(SizeForFunctionCallInfo(nargs))

/* getting arguments has a different API, so these macros unify the versions */
#define FC_ARG(fcinfo, n) ((fcinfo)->args[(n)].value)
#define FC_NULL(fcinfo, n) ((fcinfo)->args[(n)].isnull)

#endif

/* convenience setters */
#define FC_SET_ARG(fcinfo, n, val)                                                                 \
	do                                                                                             \
	{                                                                                              \
		short _n = (n);                                                                            \
		FunctionCallInfo _fcinfo = (fcinfo);                                                       \
		FC_ARG(_fcinfo, _n) = (val);                                                               \
		FC_NULL(_fcinfo, _n) = false;                                                              \
	} while (0)

#define FC_SET_NULL(fcinfo, n)                                                                     \
	do                                                                                             \
	{                                                                                              \
		short _n = (n);                                                                            \
		FunctionCallInfo _fcinfo = (fcinfo);                                                       \
		FC_ARG(_fcinfo, _n) = 0;                                                                   \
		FC_NULL(_fcinfo, _n) = true;                                                               \
	} while (0)

/*
 * In PG12 OID columns were removed changing all OID columns in the catalog to
 * be regular columns. This necessitates passing in the attnum of said column to
 * any function that wishes to access these columns. In earlier versions, this
 * parameter can be safely ignored.
 */
#if PG12_LT
#define GetSysCacheOid2Compat(cacheId, oidcol, key1, key2) GetSysCacheOid2(cacheId, key1, key2)
#else
#define GetSysCacheOid2Compat GetSysCacheOid2
#endif

/*
 * get_attname
 *
 * PG11 introduced a missing_ok boolean to the function signature. Given that
 * that seems like a useful check in many cases, we are going with forwards
 * compatibility here and have a small static inline function to replicate the
 * behavior on older versions.
 */
#if PG11_LT
static inline char *
get_attname_compat(Oid relid, AttrNumber attnum, bool missing_ok)
{
	char *name = get_attname(relid, attnum);

	if (!missing_ok && name == NULL)
		elog(ERROR, "cache lookup failed for attribute %d of relation %u", attnum, relid);
	return name;
}
#else
#define get_attname_compat get_attname
#endif

/* get_projection_info_slot */
#if PG96
#define get_projection_info_slot_compat(pinfo) ((pinfo)->pi_slot)
#else
#define get_projection_info_slot_compat(pinfo) ((pinfo)->pi_state.resultslot)
#endif

/*
 * heap_attisnull
 *
 * PG11 modified how heap tuples are accessed, especially for null values, to
 * allow for fast alters when tables have not-null default (with non-volatile
 * functions).
 * See: https://github.com/postgres/postgres/commit/16828d5c0273b4fe5f10f42588005f16b415b2d8
 * It now requires the passing in of a TupleDesc in a number of places where it
 * didn't before. We adopt the PG11 convention of passing in a TupleDesc (which
 * can be null for pg_catalog tables, but must be provided otherwise), and
 * simply omit in earlier versions.
 */
#if PG11_LT
#define heap_attisnull_compat(tup, attnum, tupledesc) heap_attisnull(tup, attnum)
#else
#define heap_attisnull_compat heap_attisnull
#endif

/*
 * index_create
 *
 * PG11 replaced several boolean arguments to index_create with a single bitmask
 * flag. Since that is generally cleaner, we adopt that convention. Several new
 * arguments were also introduced, so we introduce a compat that takes the best
 * of both worlds. (See:
 * https://github.com/postgres/postgres/commit/a61f5ab986386628cf20b33971364475ce452412)

 * Parent indexes and all support added for local partitioned indexes is unused
 * by us, we might in the future want to use some of those flags depending on how
 * we eventually decide to work with declarative partitioning.
 */
#if PG11_LT
/* Index flags */
#define INDEX_CREATE_IS_PRIMARY (1 << 0)
#define INDEX_CREATE_ADD_CONSTRAINT (1 << 1)
#define INDEX_CREATE_SKIP_BUILD (1 << 2)
#define INDEX_CREATE_CONCURRENT (1 << 3)
#define INDEX_CREATE_IF_NOT_EXISTS (1 << 4)
#define INDEX_CREATE_PARTITIONED (1 << 5)
#define INDEX_CREATE_INVALID (1 << 6)
/* Constraint flags */
#define INDEX_CONSTR_CREATE_MARK_AS_PRIMARY (1 << 0)
#define INDEX_CONSTR_CREATE_DEFERRABLE (1 << 1)
#define INDEX_CONSTR_CREATE_INIT_DEFERRED (1 << 2)
#define INDEX_CONSTR_CREATE_UPDATE_INDEX (1 << 3)
#define INDEX_CONSTR_CREATE_REMOVE_OLD_DEPS (1 << 4)

#define index_create_compat(heapRelation,                                                          \
							indexRelationName,                                                     \
							indexRelationId,                                                       \
							relFileNode,                                                           \
							indexInfo,                                                             \
							indexColNames,                                                         \
							accessMethodObjectId,                                                  \
							tableSpaceId,                                                          \
							collationObjectId,                                                     \
							classObjectId,                                                         \
							coloptions,                                                            \
							reloptions,                                                            \
							flags,                                                                 \
							constr_flags,                                                          \
							allow_system_table_mods,                                               \
							is_internal)                                                           \
	index_create(heapRelation,                                                                     \
				 indexRelationName,                                                                \
				 indexRelationId,                                                                  \
				 relFileNode,                                                                      \
				 indexInfo,                                                                        \
				 indexColNames,                                                                    \
				 accessMethodObjectId,                                                             \
				 tableSpaceId,                                                                     \
				 collationObjectId,                                                                \
				 classObjectId,                                                                    \
				 coloptions,                                                                       \
				 reloptions,                                                                       \
				 ((flags & INDEX_CREATE_IS_PRIMARY) != 0),                                         \
				 ((flags & INDEX_CREATE_ADD_CONSTRAINT) != 0),                                     \
				 ((constr_flags & INDEX_CONSTR_CREATE_DEFERRABLE) != 0),                           \
				 ((constr_flags & INDEX_CONSTR_CREATE_INIT_DEFERRED) != 0),                        \
				 allow_system_table_mods,                                                          \
				 ((flags & INDEX_CREATE_SKIP_BUILD) != 0),                                         \
				 ((flags & INDEX_CREATE_CONCURRENT) != 0),                                         \
				 is_internal,                                                                      \
				 ((flags & INDEX_CREATE_IF_NOT_EXISTS) != 0))
#else
#define index_create_compat(heapRelation,                                                          \
							indexRelationName,                                                     \
							indexRelationId,                                                       \
							relFileNode,                                                           \
							indexInfo,                                                             \
							indexColNames,                                                         \
							accessMethodObjectId,                                                  \
							tableSpaceId,                                                          \
							collationObjectId,                                                     \
							classObjectId,                                                         \
							coloptions,                                                            \
							reloptions,                                                            \
							flags,                                                                 \
							constr_flags,                                                          \
							allow_system_table_mods,                                               \
							is_internal)                                                           \
	index_create(heapRelation,                                                                     \
				 indexRelationName,                                                                \
				 indexRelationId,                                                                  \
				 InvalidOid,                                                                       \
				 InvalidOid,                                                                       \
				 relFileNode,                                                                      \
				 indexInfo,                                                                        \
				 indexColNames,                                                                    \
				 accessMethodObjectId,                                                             \
				 tableSpaceId,                                                                     \
				 collationObjectId,                                                                \
				 classObjectId,                                                                    \
				 coloptions,                                                                       \
				 reloptions,                                                                       \
				 flags,                                                                            \
				 constr_flags,                                                                     \
				 allow_system_table_mods,                                                          \
				 is_internal,                                                                      \
				 NULL)
#endif

/* InitResultRelInfo */
#if PG96
#define InitResultRelInfoCompat(result_rel_info,                                                   \
								result_rel_desc,                                                   \
								result_rel_index,                                                  \
								instrument_options)                                                \
	InitResultRelInfo(result_rel_info, result_rel_desc, result_rel_index, instrument_options)
#else
#define InitResultRelInfoCompat(result_rel_info,                                                   \
								result_rel_desc,                                                   \
								result_rel_index,                                                  \
								instrument_options)                                                \
	InitResultRelInfo(result_rel_info, result_rel_desc, result_rel_index, NULL, instrument_options)
#endif

/* make_op */
#if PG96
#define make_op_compat(pstate, opname, ltree, rtree, location)                                     \
	make_op(pstate, opname, ltree, rtree, location)
#else
#define make_op_compat(pstate, opname, ltree, rtree, location)                                     \
	make_op(pstate, opname, ltree, rtree, (pstate)->p_last_srf, location)
#endif

/* map_variable_attnos */
#if PG96
#define map_variable_attnos_compat(expr,                                                           \
								   varno,                                                          \
								   sublevels_up,                                                   \
								   map,                                                            \
								   map_size,                                                       \
								   rowtype,                                                        \
								   found_whole_row)                                                \
	map_variable_attnos(expr, varno, sublevels_up, map, map_size, found_whole_row)
#else
#define map_variable_attnos_compat(returning_clauses,                                              \
								   varno,                                                          \
								   sublevels_up,                                                   \
								   map,                                                            \
								   map_size,                                                       \
								   rowtype,                                                        \
								   found_whole_row)                                                \
	map_variable_attnos(returning_clauses,                                                         \
						varno,                                                                     \
						sublevels_up,                                                              \
						map,                                                                       \
						map_size,                                                                  \
						rowtype,                                                                   \
						found_whole_row);
#endif

/*
 * ExplainPropertyInteger
 *
 * PG11 added a unit parameter to ExplainPropertyInteger
 */
#if PG11_LT
#define ExplainPropertyIntegerCompat(label, unit, value, es)                                       \
	ExplainPropertyInteger(label, value, es)
#else
#define ExplainPropertyIntegerCompat(label, unit, value, es)                                       \
	ExplainPropertyInteger(label, unit, value, es)
#endif

/* ParseFuncOrColumn */
#if PG96
#define ParseFuncOrColumnCompat(pstate, funcname, fargs, fn, location)                             \
	ParseFuncOrColumn(pstate, funcname, fargs, fn, location)
#else
#define ParseFuncOrColumnCompat(pstate, funcname, fargs, fn, location)                             \
	ParseFuncOrColumn(pstate, funcname, fargs, (pstate)->p_last_srf, fn, location)
#endif

/*
 * PG_RETURN_JSONB -> PG_RETURN_JSONB_P
 *
 * PG11 fixes some functions that return pointers to follow convention and end
 * with P.
 */
#if PG11_LT
#define PG_RETURN_JSONB_P PG_RETURN_JSONB
#endif

/*
 * PG11 introduced a new level of nodes inside of ResultRelInfo for dealing with
 * ON CONFLICT behavior in partitions (see:
 * https://github.com/postgres/postgres/commit/555ee77a9668e3f1b03307055b5027e13bf1a715).
 * Our compat functions act as an accessor/setter for these fields, whenever they
 * are nested.
 */

#if PG11_LT
#define ResultRelInfo_OnConflictProjInfoCompat(rri) (rri)->ri_onConflictSetProj
#define ResultRelInfo_OnConflictWhereCompat(rri) (rri)->ri_onConflictSetWhere
#define ResultRelInfo_OnConflictNotNull(rri) true
#else
#define ResultRelInfo_OnConflictProjInfoCompat(rri) (rri)->ri_onConflict->oc_ProjInfo
#define ResultRelInfo_OnConflictWhereCompat(rri) (rri)->ri_onConflict->oc_WhereClause
#define ResultRelInfo_OnConflictNotNull(rri) (rri)->ri_onConflict != NULL
#endif

/* RangeVarGetRelidExtended
 *
 * PG11 replaced several boolean arguments to RangeVarGetRelidExtended with a single bitmask flag.
 * Since that is generally cleaner, we adopt that convention. (See:
 * https://github.com/postgres/postgres/commit/d87510a524f36a630cfb34cc392e95e959a1b0dc) We do not
 * define RVR_SKIP_LOCKED as cannot yet emulate it
 */
#if PG11_LT
#define RVR_MISSING_OK (1 << 0)
#define RVR_NOWAIT (1 << 1)
#define RangeVarGetRelidExtendedCompat(relation, lockmode, flags, callback, callback_arg)          \
	RangeVarGetRelidExtended(relation,                                                             \
							 lockmode,                                                             \
							 (flags & RVR_MISSING_OK) != 0,                                        \
							 (flags & RVR_NOWAIT) != 0,                                            \
							 callback,                                                             \
							 callback_arg)
#else
#define RangeVarGetRelidExtendedCompat RangeVarGetRelidExtended
#endif

/* RenameRelationInternal
 */
#if PG12_LT
#define RenameRelationInternalCompat(relid, name, is_internal, is_index)                           \
	RenameRelationInternal(relid, name, is_internal)
#else
#define RenameRelationInternalCompat RenameRelationInternal
#endif

/*
 * TransactionChain -> TransactionBlock
 *
 * PG11 renames FooTransactionChain -> FooTransactionBlock. (See:
 * https://github.com/postgres/postgres/commit/04700b685f31508036456bea4d92533e5ceee9d6).
 * Map them back to TransactionChain for previous versions. (We currently only
 * use one of the functions in this family, but making this general in case we
 * use the others in the future).
 */
#if PG11_LT
#define PreventInTransactionBlock PreventTransactionChain
#endif

/*
 * TupleDescAttr
 *
 * TupleDescAttr was only backpatched to 9.6.5. Make it work under 9.6.3 and 9.6.4
 */
#if ((PG_VERSION_NUM >= 90603) && PG_VERSION_NUM < 90605)
#define TupleDescAttr(tupdesc, i) ((tupdesc)->attrs[(i)])
#endif

/* WaitLatch */
#if PG96
#define WaitLatchCompat(latch, wakeEvents, timeout) WaitLatch(latch, wakeEvents, timeout)

extern int oid_cmp(const void *p1, const void *p2);

#else
#define WaitLatchCompat(latch, wakeEvents, timeout)                                                \
	WaitLatch(latch, wakeEvents, timeout, PG_WAIT_EXTENSION)
#endif

/* create_merge_append_path */
#if PG96
#define create_merge_append_path_compat(root, rel, merge_childs, pathkeys, subpath)                \
	create_merge_append_path(root, rel, merge_childs, pathkeys, subpath)
#else
#define create_merge_append_path_compat(root, rel, merge_childs, pathkeys, subpath)                \
	create_merge_append_path(root, rel, merge_childs, pathkeys, subpath, NIL)
#endif

/* pq_sendint is deprecated in PG11, so create pq_sendint32 in 9.6 and 10 */
#if PG11_LT
#define pq_sendint32(buf, i) pq_sendint(buf, i, 4)
#endif

/* create this function for symmetry with above */
#define pq_getmsgint32(buf) pq_getmsgint(buf, 4)

#if PG12_LT
#define TUPLE_DESC_HAS_OIDS(desc) (desc)->tdhasoid
#else
#define TUPLE_DESC_HAS_OIDS(desc) false
#endif

#if PG96
#if __GNUC__ >= 3
#define likely(x) __builtin_expect((x) != 0, 1)
#define unlikely(x) __builtin_expect((x) != 0, 0)
#else
#define likely(x) ((x) != 0)
#define unlikely(x) ((x) != 0)
#endif
#endif

/* Compatibility functions for table access method API introduced in PG12 */
#if PG12_LT
#include "compat/tupconvert.h"
#include "compat/tuptable.h"
#include "compat/tableam.h"

#else
#define ts_tuptableslot_set_table_oid(slot, table_oid) (slot)->tts_tableOid = table_oid
#endif

#if PG12_GE
#define ExecTypeFromTLCompat(tlist, hasoid) ExecTypeFromTL(tlist)
#else
#define ExecTypeFromTLCompat(tlist, hasoid) ExecTypeFromTL(tlist, hasoid)
#endif

/* backport pg_add_s64_overflow/pg_sub_s64_overflow */
#if PG11_LT
static inline bool
pg_add_s64_overflow(int64 a, int64 b, int64 *result)
{
#if defined(HAVE__BUILTIN_OP_OVERFLOW)
	return __builtin_add_overflow(a, b, result);
#elif defined(HAVE_INT128)
	int128 res = (int128) a + (int128) b;

	if (res > PG_INT64_MAX || res < PG_INT64_MIN)
	{
		*result = 0x5EED; /* to avoid spurious warnings */
		return true;
	}
	*result = (int64) res;
	return false;
#else
	if ((a > 0 && b > 0 && a > PG_INT64_MAX - b) || (a < 0 && b < 0 && a < PG_INT64_MIN - b))
	{
		*result = 0x5EED; /* to avoid spurious warnings */
		return true;
	}
	*result = a + b;
	return false;
#endif
}

/*
 * If a - b overflows, return true, otherwise store the result of a - b into
 * *result. The content of *result is implementation defined in case of
 * overflow.
 */
static inline bool
pg_sub_s64_overflow(int64 a, int64 b, int64 *result)
{
#if defined(HAVE__BUILTIN_OP_OVERFLOW)
	return __builtin_sub_overflow(a, b, result);
#elif defined(HAVE_INT128)
	int128 res = (int128) a - (int128) b;

	if (res > PG_INT64_MAX || res < PG_INT64_MIN)
	{
		*result = 0x5EED; /* to avoid spurious warnings */
		return true;
	}
	*result = (int64) res;
	return false;
#else
	if ((a < 0 && b > 0 && a < PG_INT64_MIN + b) || (a > 0 && b < 0 && a > PG_INT64_MAX + b))
	{
		*result = 0x5EED; /* to avoid spurious warnings */
		return true;
	}
	*result = a - b;
	return false;
#endif
}
#endif

/* Backport of list_qsort() */
#if PG11_LT
typedef int (*list_qsort_comparator)(const void *a, const void *b);
/*
 * Sort a list as though by qsort.
 *
 * A new list is built and returned.  Like list_copy, this doesn't make
 * fresh copies of any pointed-to data.
 *
 * The comparator function receives arguments of type ListCell **.
 */

static List *
new_list(NodeTag type)
{
	List *new_list;
	ListCell *new_head;

	new_head = (ListCell *) palloc(sizeof(*new_head));
	new_head->next = NULL;
	/* new_head->data is left undefined! */

	new_list = (List *) palloc(sizeof(*new_list));
	new_list->type = type;
	new_list->length = 1;
	new_list->head = new_head;
	new_list->tail = new_head;

	return new_list;
}

static inline List *
list_qsort(const List *list, list_qsort_comparator cmp)
{
	int len = list_length(list);
	ListCell **list_arr;
	List *newlist;
	ListCell *newlist_prev;
	ListCell *cell;
	int i;

	/* Empty list is easy */
	if (len == 0)
		return NIL;

	/* Flatten list cells into an array, so we can use qsort */
	list_arr = (ListCell **) palloc(sizeof(ListCell *) * len);
	i = 0;
	foreach (cell, list)
		list_arr[i++] = cell;

	qsort(list_arr, len, sizeof(ListCell *), cmp);

	/* Construct new list (this code is much like list_copy) */
	newlist = new_list(list->type);
	newlist->length = len;

	/*
	 * Copy over the data in the first cell; new_list() has already allocated
	 * the head cell itself
	 */
	newlist->head->data = list_arr[0]->data;

	newlist_prev = newlist->head;
	for (i = 1; i < len; i++)
	{
		ListCell *newlist_cur;

		newlist_cur = (ListCell *) palloc(sizeof(*newlist_cur));
		newlist_cur->data = list_arr[i]->data;
		newlist_prev->next = newlist_cur;

		newlist_prev = newlist_cur;
	}

	newlist_prev->next = NULL;
	newlist->tail = newlist_prev;

	/* Might as well free the workspace array */
	pfree(list_arr);

	return newlist;
}

#endif

/*
 * ForeignKeyCacheInfo doesn't contain the constraint Oid in early versions.
 * This is a fix for PG10 and PG96 until support for them is gone.
 */
#if PG11_LT
#define RelationGetFKeyListCompat(rel) ts_relation_get_fk_list(rel)
#define T_ForeignKeyCacheInfoCompat T_ForeignKeyCacheInfo
typedef struct ForeignKeyCacheInfoCompat
{
	ForeignKeyCacheInfo base;
	Oid conoid;
} ForeignKeyCacheInfoCompat;
/* No need to copy FK list, since custom implementation doesn't use cache. */
#define copy_fk_list_from_cache(l) l
#else
#define RelationGetFKeyListCompat(rel) RelationGetFKeyList(rel)
#define ForeignKeyCacheInfoCompat ForeignKeyCacheInfo
/* Copies FK list, since the cache can be invalidated. */
#define copy_fk_list_from_cache(l) copyObject(l)
#endif /* PG11_LT */

#endif /* TIMESCALEDB_COMPAT_H */
