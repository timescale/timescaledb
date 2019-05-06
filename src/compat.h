/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_COMPAT_H
#define TIMESCALEDB_COMPAT_H

#include <postgres.h>
#include <pgstat.h>
#include <utils/lsyscache.h>
#include <executor/executor.h>
#include <executor/tuptable.h>
#include <nodes/execnodes.h>
#include <nodes/nodes.h>

#include "export.h"
#include "planner_import.h"

#define is_supported_pg_version_96(version) ((version >= 90603) && (version < 100000))
#define is_supported_pg_version_10(version) ((version >= 100002) && (version < 110000))
#define is_supported_pg_version_11(version) ((version >= 110000) && (version < 120000))
#define is_supported_pg_version(version)                                                           \
	(is_supported_pg_version_96(version) || is_supported_pg_version_10(version) ||                 \
	 is_supported_pg_version_11(version))

#define PG96 is_supported_pg_version_96(PG_VERSION_NUM)
#define PG10 is_supported_pg_version_10(PG_VERSION_NUM)
#define PG11 is_supported_pg_version_11(PG_VERSION_NUM)
#define PG10_LT PG96
#define PG10_GE !(PG10_LT)
#define PG11_LT (PG96 || PG10)
#define PG11_GE !(PG11_LT)

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
#if PG96 || PG10
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
#if PG96 || PG10
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
#if PG96 || PG10
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
#if PG96 || PG10
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

/* ExecARInsertTriggers */
#if PG96
#define ExecARInsertTriggersCompat(estate, result_rel_info, tuple, recheck_indexes)                \
	ExecARInsertTriggers(estate, result_rel_info, tuple, recheck_indexes)
#else
#define ExecARInsertTriggersCompat(estate, result_rel_info, tuple, recheck_indexes)                \
	ExecARInsertTriggers(estate, result_rel_info, tuple, recheck_indexes, NULL)
#endif

/* ExecASInsertTriggers */
#if PG96
#define ExecASInsertTriggersCompat(estate, result_rel_info)                                        \
	ExecASInsertTriggers(estate, result_rel_info)
#else
#define ExecASInsertTriggersCompat(estate, result_rel_info)                                        \
	ExecASInsertTriggers(estate, result_rel_info, NULL)
#endif

/* ExecBuildProjectionInfo */
#if PG96
#define ExecBuildProjectionInfoCompat(tl, exprContext, slot, parent, inputdesc)                    \
	ExecBuildProjectionInfo((List *) ExecInitExpr((Expr *) tl, NULL), exprContext, slot, inputdesc)
#else
#define ExecBuildProjectionInfoCompat(tl, exprContext, slot, parent, inputdesc)                    \
	ExecBuildProjectionInfo(tl, exprContext, slot, parent, inputdesc)
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
#if PG96 || PG10
static inline TupleTableSlot *
ExecInitExtraTupleSlotCompat(EState *estate, TupleDesc tupdesc)
{
	TupleTableSlot *myslot = ExecInitExtraTupleSlot(estate);

	if (tupdesc != NULL)
		ExecSetSlotDescriptor(myslot, tupdesc);

	return myslot;
}

/*
 * ExecSetTupleBound is only available starting with PG11 so we map to a backported version
 * for PG9.6 and PG10
 */
#if PG96 || PG10
#define ExecSetTupleBound(tuples_needed, child_node) ts_ExecSetTupleBound(tuples_needed, child_node)
#endif

static inline TupleTableSlot *
MakeTupleTableSlotCompat(TupleDesc tupdesc)
{
	TupleTableSlot *myslot = MakeTupleTableSlot();

	if (tupdesc != NULL)
		ExecSetSlotDescriptor(myslot, tupdesc);

	return myslot;
}
#else
#define ExecInitExtraTupleSlotCompat ExecInitExtraTupleSlot
#define MakeTupleTableSlotCompat MakeTupleTableSlot
#endif

/*
 * get_attname
 *
 * PG11 introduced a missing_ok boolean to the function signature. Given that
 * that seems like a useful check in many cases, we are going with forwards
 * compatibility here and have a small static inline function to replicate the
 * behavior on older versions.
 */
#if PG96 || PG10
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
#if PG96 || PG10
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
#if PG96 || PG10
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
#if PG96 || PG10
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
#if PG96 || PG10
#define PG_RETURN_JSONB_P PG_RETURN_JSONB
#endif

/*
 * PG11 introduced a new level of nodes inside of ResultRelInfo for dealing with
 * ON CONFLICT behavior in partitions (see:
 * https://github.com/postgres/postgres/commit/555ee77a9668e3f1b03307055b5027e13bf1a715).
 * Our compat functions act as an accessor/setter for these fields, whenever they
 * are nested.
 */

#if PG96 || PG10
#define ResultRelInfo_OnConflictProjInfoCompat(rri) ((rri)->ri_onConflictSetProj)
#define ResultRelInfo_OnConflictWhereCompat(rri) ((rri)->ri_onConflictSetWhere)
#define ResultRelInfo_OnConflictNotNull(rri) true
#else
#define ResultRelInfo_OnConflictProjInfoCompat(rri) ((rri)->ri_onConflict->oc_ProjInfo)
#define ResultRelInfo_OnConflictWhereCompat(rri) ((rri)->ri_onConflict->oc_WhereClause)
#define ResultRelInfo_OnConflictNotNull(rri) ((rri)->ri_onConflict != NULL)
#endif

/* RangeVarGetRelidExtended
 *
 * PG11 replaced several boolean arguments to RangeVarGetRelidExtended with a single bitmask flag.
 * Since that is generally cleaner, we adopt that convention. (See:
 * https://github.com/postgres/postgres/commit/d87510a524f36a630cfb34cc392e95e959a1b0dc) We do not
 * define RVR_SKIP_LOCKED as cannot yet emulate it
 */
#if PG96 || PG10
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

/*
 * TransactionChain -> TransactionBlock
 *
 * PG11 renames FooTransactionChain -> FooTransactionBlock. (See:
 * https://github.com/postgres/postgres/commit/04700b685f31508036456bea4d92533e5ceee9d6).
 * Map them back to TransactionChain for previous versions. (We currently only
 * use one of the functions in this family, but making this general in case we
 * use the others in the future).
 */
#if PG96 || PG10
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

/* pq_sendint is deprecated in PG11, so create pq_sendint32 in 9.6 and 10 */
#if PG96 || PG10
#define pq_sendint32(buf, i) pq_sendint(buf, i, 4)
#endif

/* create this function for symmetry with above */
#define pq_getmsgint32(buf) pq_getmsgint(buf, 4)

#if PG96
#if __GNUC__ >= 3
#define likely(x) __builtin_expect((x) != 0, 1)
#define unlikely(x) __builtin_expect((x) != 0, 0)
#else
#define likely(x) ((x) != 0)
#define unlikely(x) ((x) != 0)
#endif
#endif

#endif /* TIMESCALEDB_COMPAT_H */
