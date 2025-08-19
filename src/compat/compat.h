/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include <commands/cluster.h>
#include <commands/defrem.h>
#include <commands/explain.h>
#include <commands/trigger.h>
#include <commands/vacuum.h>
#include <executor/executor.h>
#include <executor/tuptable.h>
#include <nodes/execnodes.h>
#include <nodes/nodes.h>
#include <optimizer/restrictinfo.h>
#include <pgstat.h>
#include <storage/lmgr.h>
#include <utils/jsonb.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>

#include "export.h"

#define PG_MAJOR_MIN 15

/*
 * Prevent building against upstream versions that had ABI breaking change (15.9, 16.5, 17.1)
 * that was reverted in the following release.
 */

#define is_supported_pg_version_15(version) ((version >= 150010) && (version < 160000))
#define is_supported_pg_version_16(version) ((version >= 160006) && (version < 170000))
#define is_supported_pg_version_17(version) ((version >= 170002) && (version < 180000))
#define is_supported_pg_version_18(version) ((version >= 180000) && (version < 190000))

/*
 * To compile with an unsupported version, use -DEXPERIMENTAL=ON with cmake.
 * (Useful when testing with unreleased versions)
 */
#define is_supported_pg_version(version)                                                           \
	(is_supported_pg_version_15(version) || is_supported_pg_version_16(version) ||                 \
	 is_supported_pg_version_17(version) || is_supported_pg_version_18(version))

#define PG15 is_supported_pg_version_15(PG_VERSION_NUM)
#define PG16 is_supported_pg_version_16(PG_VERSION_NUM)
#define PG17 is_supported_pg_version_17(PG_VERSION_NUM)
#define PG18 is_supported_pg_version_18(PG_VERSION_NUM)

#define PG15_LT (PG_VERSION_NUM < 150000)
#define PG15_GE (PG_VERSION_NUM >= 150000)
#define PG16_LT (PG_VERSION_NUM < 160000)
#define PG16_GE (PG_VERSION_NUM >= 160000)
#define PG17_LT (PG_VERSION_NUM < 170000)
#define PG17_GE (PG_VERSION_NUM >= 170000)
#define PG18_LT (PG_VERSION_NUM < 180000)
#define PG18_GE (PG_VERSION_NUM >= 180000)

#if !(is_supported_pg_version(PG_VERSION_NUM))
#error "Unsupported PostgreSQL version"
#endif

#if ((PG_VERSION_NUM >= 150009 && PG_VERSION_NUM < 160000) ||                                      \
	 (PG_VERSION_NUM >= 160005 && PG_VERSION_NUM < 170000) || (PG_VERSION_NUM >= 170001))
/*
 * The above versions introduced a fix for potentially losing updates to
 * pg_class and pg_database due to inplace updates done to those catalog
 * tables by PostgreSQL. The fix requires taking a lock on the tuple via
 * SearchSysCacheLocked1(). For older PG versions, we just map the new
 * function to the unlocked version and the unlocking of the tuple is a noop.
 *
 * https://github.com/postgres/postgres/commit/3b7a689e1a805c4dac2f35ff14fd5c9fdbddf150
 *
 * Here's an excerpt from README.tuplock that explains the need for additional
 * tuple locks:
 *
 * If IsInplaceUpdateRelation() returns true for a table, the table is a
 * system catalog that receives systable_inplace_update_begin() calls.
 * Preparing a heap_update() of these tables follows additional locking rules,
 * to ensure we don't lose the effects of an inplace update. In particular,
 * consider a moment when a backend has fetched the old tuple to modify, not
 * yet having called heap_update(). Another backend's inplace update starting
 * then can't conclude until the heap_update() places its new tuple in a
 * buffer. We enforce that using locktags as follows. While DDL code is the
 * main audience, the executor follows these rules to make e.g. "MERGE INTO
 * pg_class" safer. Locking rules are per-catalog:
 *
 * pg_class heap_update() callers: before copying the tuple to modify, take a
 * lock on the tuple, a ShareUpdateExclusiveLock on the relation, or a
 * ShareRowExclusiveLock or stricter on the relation.
 */
#define SYSCACHE_TUPLE_LOCK_NEEDED 1
#define AssertSufficientPgClassUpdateLockHeld(relid)                                               \
	Assert(CheckRelationOidLockedByMe(relid, ShareUpdateExclusiveLock, false) ||                   \
		   CheckRelationOidLockedByMe(relid, ShareRowExclusiveLock, true));
#define UnlockSysCacheTuple(rel, tid) UnlockTuple(rel, tid, InplaceUpdateTupleLock);
#else
#define SearchSysCacheLockedCopy1(rel, datum) SearchSysCacheCopy1(rel, datum)
#define UnlockSysCacheTuple(rel, tid)
#define AssertSufficientPgClassUpdateLockHeld(relid)
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

#if PG16_LT
#define ExecInsertIndexTuplesCompat(rri,                                                           \
									slot,                                                          \
									estate,                                                        \
									update,                                                        \
									noDupErr,                                                      \
									specConflict,                                                  \
									arbiterIndexes,                                                \
									onlySummarizing)                                               \
	ExecInsertIndexTuples(rri, slot, estate, update, noDupErr, specConflict, arbiterIndexes)
#else
#define ExecInsertIndexTuplesCompat(rri,                                                           \
									slot,                                                          \
									estate,                                                        \
									update,                                                        \
									noDupErr,                                                      \
									specConflict,                                                  \
									arbiterIndexes,                                                \
									onlySummarizing)                                               \
	ExecInsertIndexTuples(rri,                                                                     \
						  slot,                                                                    \
						  estate,                                                                  \
						  update,                                                                  \
						  noDupErr,                                                                \
						  specConflict,                                                            \
						  arbiterIndexes,                                                          \
						  onlySummarizing)
#endif

/*
 * PG16 removed outerjoin_delayed, nullable_relids arguments from make_restrictinfo
 * https://github.com/postgres/postgres/commit/b448f1c8d8
 *
 * PG16 adds three new parameter - has_clone, is_clone and incompatible_relids, as a
 * part of fixing the filtering of "cloned" outer-join quals
 * https://github.com/postgres/postgres/commit/991a3df227
 */

#if PG16_LT
#define make_restrictinfo_compat(root,                                                             \
								 clause,                                                           \
								 is_pushed_down,                                                   \
								 has_clone,                                                        \
								 is_clone,                                                         \
								 outerjoin_delayed,                                                \
								 pseudoconstant,                                                   \
								 security_level,                                                   \
								 required_relids,                                                  \
								 incompatible_relids,                                              \
								 outer_relids,                                                     \
								 nullable_relids)                                                  \
	make_restrictinfo(root,                                                                        \
					  clause,                                                                      \
					  is_pushed_down,                                                              \
					  outerjoin_delayed,                                                           \
					  pseudoconstant,                                                              \
					  security_level,                                                              \
					  required_relids,                                                             \
					  outer_relids,                                                                \
					  nullable_relids)
#else
#define make_restrictinfo_compat(root,                                                             \
								 clause,                                                           \
								 is_pushed_down,                                                   \
								 has_clone,                                                        \
								 is_clone,                                                         \
								 outerjoin_delayed,                                                \
								 pseudoconstant,                                                   \
								 security_level,                                                   \
								 required_relids,                                                  \
								 incompatible_relids,                                              \
								 outer_relids,                                                     \
								 nullable_relids)                                                  \
	make_restrictinfo(root,                                                                        \
					  clause,                                                                      \
					  is_pushed_down,                                                              \
					  has_clone,                                                                   \
					  is_clone,                                                                    \
					  pseudoconstant,                                                              \
					  security_level,                                                              \
					  required_relids,                                                             \
					  incompatible_relids,                                                         \
					  outer_relids)
#endif

/* fmgr
 * In a9c35cf postgres changed how it calls SQL functions so that the number of
 * argument-slots allocated is chosen dynamically, instead of being fixed. This
 * change was ABI-breaking, so we cannot backport this optimization, however,
 * we do backport the interface, so that all our code will be compatible with
 * new versions.
 */

/* convenience macro to allocate FunctionCallInfoData on the heap */
#define HEAP_FCINFO(nargs) palloc(SizeForFunctionCallInfo(nargs))

/* getting arguments has a different API, so these macros unify the versions */
#define FC_ARG(fcinfo, n) ((fcinfo)->args[(n)].value)
#define FC_NULL(fcinfo, n) ((fcinfo)->args[(n)].isnull)

#define FC_FN_OID(fcinfo) ((fcinfo)->flinfo->fn_oid)

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

/* create this function for symmetry with pq_sendint32 */
#define pq_getmsgint32(buf) pq_getmsgint(buf, 4)

#define ts_tuptableslot_set_table_oid(slot, table_oid) (slot)->tts_tableOid = table_oid

static inline ClusterParams *
get_cluster_options(const ClusterStmt *stmt)
{
	ListCell *lc;
	ClusterParams *params = palloc0(sizeof(ClusterParams));
	bool verbose = false;

	/* Parse option list */
	foreach (lc, stmt->params)
	{
		DefElem *opt = (DefElem *) lfirst(lc);
		if (strcmp(opt->defname, "verbose") == 0)
			verbose = defGetBoolean(opt);
		else
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unrecognized CLUSTER option \"%s\"", opt->defname),
					 parser_errposition(NULL, opt->location)));
	}

	params->options = (verbose ? CLUOPT_VERBOSE : 0);

	return params;
}

#include <catalog/index.h>

static inline int
get_reindex_options(ReindexStmt *stmt)
{
	ListCell *lc;
	bool concurrently = false;
	bool verbose = false;

	/* Parse option list */
	foreach (lc, stmt->params)
	{
		DefElem *opt = (DefElem *) lfirst(lc);
		if (strcmp(opt->defname, "verbose") == 0)
			verbose = defGetBoolean(opt);
		else if (strcmp(opt->defname, "concurrently") == 0)
			concurrently = defGetBoolean(opt);
		else
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unrecognized REINDEX option \"%s\"", opt->defname),
					 parser_errposition(NULL, opt->location)));
	}
	return (verbose ? REINDEXOPT_VERBOSE : 0) | (concurrently ? REINDEXOPT_CONCURRENTLY : 0);
}

/*
 * define some list macros for convenience
 */
#define lfifth(l) lfirst(list_nth_cell(l, 4))
#define lfifth_int(l) lfirst_int(list_nth_cell(l, 4))

#if PG16_LT
/*
 * PG15 consolidate VACUUM xid cutoff logic.
 *
 * https://github.com/postgres/postgres/commit/efa4a946
 *
 * PG16 introduced VacuumCutoffs so define here for previous PG versions.
 */
struct VacuumCutoffs
{
	TransactionId relfrozenxid;
	MultiXactId relminmxid;
	TransactionId OldestXmin;
	MultiXactId OldestMxact;
	TransactionId FreezeLimit;
	MultiXactId MultiXactCutoff;
};

static inline bool
vacuum_get_cutoffs(Relation rel, const VacuumParams *params, struct VacuumCutoffs *cutoffs)
{
	return vacuum_set_xid_limits(rel,
								 0,
								 0,
								 0,
								 0,
								 &cutoffs->OldestXmin,
								 &cutoffs->OldestMxact,
								 &cutoffs->FreezeLimit,
								 &cutoffs->MultiXactCutoff);
}
#endif

/*
 * PG16 adds TMResult argument to ExecBRUpdateTriggers
 * https://github.com/postgres/postgres/commit/7103ebb7
 * this was backported to PG15 in
 * https://github.com/postgres/postgres/commit/7d9a75713ab9
 */
#if PG15
#define ExecBRUpdateTriggers(estate,                                                               \
							 epqstate,                                                             \
							 resultRelInfo,                                                        \
							 tupleid,                                                              \
							 oldtuple,                                                             \
							 slot,                                                                 \
							 result,                                                               \
							 tmfdp)                                                                \
	ExecBRUpdateTriggersNew(estate, epqstate, resultRelInfo, tupleid, oldtuple, slot, result, tmfdp)
#endif

/*
 * PG16 adds TMResult argument to ExecBRDeleteTriggers
 * https://github.com/postgres/postgres/commit/9321c79c
 * this was backported to PG15 in
 * https://github.com/postgres/postgres/commit/7d9a75713ab9
 */
#if PG15
#define ExecBRDeleteTriggers(estate,                                                               \
							 epqstate,                                                             \
							 relinfo,                                                              \
							 tupleid,                                                              \
							 fdw_trigtuple,                                                        \
							 epqslot,                                                              \
							 tmresult,                                                             \
							 tmfd)                                                                 \
	ExecBRDeleteTriggersNew(estate,                                                                \
							epqstate,                                                              \
							relinfo,                                                               \
							tupleid,                                                               \
							fdw_trigtuple,                                                         \
							epqslot,                                                               \
							tmresult,                                                              \
							tmfd)
#endif

#if PG16_GE
#define pgstat_get_local_beentry_by_index_compat(idx) pgstat_get_local_beentry_by_index(idx)
#else
#define pgstat_get_local_beentry_by_index_compat(idx) pgstat_fetch_stat_local_beentry(idx)
#endif

/*
 * PG16 adds a new parameter to DefineIndex, total_parts, that takes
 * in the total number of direct and indirect partitions of the relation.
 *
 * https://github.com/postgres/postgres/commit/27f5c712
 */
#if PG16_LT
#define DefineIndexCompat(relationId,                                                              \
						  stmt,                                                                    \
						  indexRelationId,                                                         \
						  parentIndexId,                                                           \
						  parentConstraintId,                                                      \
						  total_parts,                                                             \
						  is_alter_table,                                                          \
						  check_rights,                                                            \
						  check_not_in_use,                                                        \
						  skip_build,                                                              \
						  quiet)                                                                   \
	DefineIndex(relationId,                                                                        \
				stmt,                                                                              \
				indexRelationId,                                                                   \
				parentIndexId,                                                                     \
				parentConstraintId,                                                                \
				is_alter_table,                                                                    \
				check_rights,                                                                      \
				check_not_in_use,                                                                  \
				skip_build,                                                                        \
				quiet)
#else
#define DefineIndexCompat(relationId,                                                              \
						  stmt,                                                                    \
						  indexRelationId,                                                         \
						  parentIndexId,                                                           \
						  parentConstraintId,                                                      \
						  total_parts,                                                             \
						  is_alter_table,                                                          \
						  check_rights,                                                            \
						  check_not_in_use,                                                        \
						  skip_build,                                                              \
						  quiet)                                                                   \
	DefineIndex(relationId,                                                                        \
				stmt,                                                                              \
				indexRelationId,                                                                   \
				parentIndexId,                                                                     \
				parentConstraintId,                                                                \
				total_parts,                                                                       \
				is_alter_table,                                                                    \
				check_rights,                                                                      \
				check_not_in_use,                                                                  \
				skip_build,                                                                        \
				quiet)
#endif

#if PG16_LT
#include <catalog/pg_database_d.h>
#include <catalog/pg_foreign_server_d.h>
#include <catalog/pg_namespace_d.h>
#include <catalog/pg_proc_d.h>
#include <catalog/pg_tablespace_d.h>
#include <utils/acl.h>

/*
 * PG16 replaces most aclcheck functions with a common object_aclcheck() function
 * https://github.com/postgres/postgres/commit/c727f511
 */
static inline AclResult
object_aclcheck(Oid classid, Oid objectid, Oid roleid, AclMode mode)
{
	switch (classid)
	{
		case DatabaseRelationId:
			return pg_database_aclcheck(objectid, roleid, mode);
		case ForeignServerRelationId:
			return pg_foreign_server_aclcheck(objectid, roleid, mode);
		case NamespaceRelationId:
			return pg_namespace_aclcheck(objectid, roleid, mode);
		case ProcedureRelationId:
			return pg_proc_aclcheck(objectid, roleid, mode);
		case TableSpaceRelationId:
			return pg_tablespace_aclcheck(objectid, roleid, mode);
		default:
			Assert(false);
	}
	return ACLCHECK_NOT_OWNER;
}

/*
 * PG16 replaces pg_foo_ownercheck() functions with a common object_ownercheck() function
 * https://github.com/postgres/postgres/commit/afbfc029
 */
static inline bool
object_ownercheck(Oid classid, Oid objectid, Oid roleid)
{
	switch (classid)
	{
		case RelationRelationId:
			return pg_class_ownercheck(objectid, roleid);
		default:
			Assert(false);
	}
	return false;
}
#endif

#if PG17_LT
/*
 * Backport of RestrictSearchPath() from PG17
 *
 * We skip the check for IsBootstrapProcessingMode() since it creates problems
 * on Windows builds and we don't need it for our use case.
 */
#include <utils/guc.h>
static inline void
RestrictSearchPath(void)
{
	set_config_option("search_path",
					  "pg_catalog, pg_temp",
					  PGC_USERSET,
					  PGC_S_SESSION,
					  GUC_ACTION_SAVE,
					  true,
					  0,
					  false);
}

/* This macro was renamed in PG17, see 414f6c0fb79a */
#define WAIT_EVENT_MESSAGE_QUEUE_INTERNAL WAIT_EVENT_MQ_INTERNAL

/* 'flush' argument was added in 173b56f1ef59 */
#define LogLogicalMessageCompat(prefix, message, size, transactional, flush)                       \
	LogLogicalMessage(prefix, message, size, transactional)

/* 'stmt' argument was added in f21848de2013 */
#define reindex_relation_compat(stmt, relid, flags, params) reindex_relation(relid, flags, params)

/* 'mergeActions' argument was added in 5f2e179bd31e */
#define CheckValidResultRelCompat(resultRelInfo, operation, mergeActions)                          \
	CheckValidResultRel(resultRelInfo, operation)

/* 'vacuum_is_relation_owner' was renamed to 'vacuum_is_permitted_for_relation' in ecb0fd33720f */
#define vacuum_is_permitted_for_relation_compat(relid, reltuple, options)                          \
	vacuum_is_relation_owner(relid, reltuple, options)

/*
 * 'BackendIdGetProc' was renamed to 'ProcNumberGetProc' in 024c52111757.
 * Also 'backendId' was renamed to 'procNumber'
 */
#define VirtualTransactionGetProcCompat(vxid) BackendIdGetProc((vxid)->backendId)

/*
 * 'opclassOptions' argument was added in 784162357130.
 * Previously indexInfo->ii_OpclassOptions was used instead.
 * On top of that 'stattargets' argument was added in 6a004f1be87d.
 */
#define index_create_compat(heapRelation,                                                          \
							indexRelationName,                                                     \
							indexRelationId,                                                       \
							parentIndexRelid,                                                      \
							parentConstraintId,                                                    \
							relFileNumber,                                                         \
							indexInfo,                                                             \
							indexColNames,                                                         \
							accessMethodId,                                                        \
							tableSpaceId,                                                          \
							collationIds,                                                          \
							opclassIds,                                                            \
							opclassOptions,                                                        \
							coloptions,                                                            \
							stattargets,                                                           \
							reloptions,                                                            \
							flags,                                                                 \
							constr_flags,                                                          \
							allow_system_table_mods,                                               \
							is_internal,                                                           \
							constraintId)                                                          \
	index_create(heapRelation,                                                                     \
				 indexRelationName,                                                                \
				 indexRelationId,                                                                  \
				 parentIndexRelid,                                                                 \
				 parentConstraintId,                                                               \
				 relFileNumber,                                                                    \
				 indexInfo,                                                                        \
				 indexColNames,                                                                    \
				 accessMethodId,                                                                   \
				 tableSpaceId,                                                                     \
				 collationIds,                                                                     \
				 opclassIds,                                                                       \
				 coloptions,                                                                       \
				 reloptions,                                                                       \
				 flags,                                                                            \
				 constr_flags,                                                                     \
				 allow_system_table_mods,                                                          \
				 is_internal,                                                                      \
				 constraintId)

#else /* PG17_GE */

#define LogLogicalMessageCompat(prefix, message, size, transactional, flush)                       \
	LogLogicalMessage(prefix, message, size, transactional, flush)

#define reindex_relation_compat(stmt, relid, flags, params)                                        \
	reindex_relation(stmt, relid, flags, params)

#define CheckValidResultRelCompat(resultRelInfo, operation, mergeActions)                          \
	CheckValidResultRel(resultRelInfo, operation, mergeActions)

#define vacuum_is_permitted_for_relation_compat(relid, reltuple, options)                          \
	vacuum_is_permitted_for_relation(relid, reltuple, options)

#define VirtualTransactionGetProcCompat(vxid) ProcNumberGetProc(vxid->procNumber)

#define index_create_compat(heapRelation,                                                          \
							indexRelationName,                                                     \
							indexRelationId,                                                       \
							parentIndexRelid,                                                      \
							parentConstraintId,                                                    \
							relFileNumber,                                                         \
							indexInfo,                                                             \
							indexColNames,                                                         \
							accessMethodId,                                                        \
							tableSpaceId,                                                          \
							collationIds,                                                          \
							opclassIds,                                                            \
							opclassOptions,                                                        \
							coloptions,                                                            \
							stattargets,                                                           \
							reloptions,                                                            \
							flags,                                                                 \
							constr_flags,                                                          \
							allow_system_table_mods,                                               \
							is_internal,                                                           \
							constraintId)                                                          \
	index_create(heapRelation,                                                                     \
				 indexRelationName,                                                                \
				 indexRelationId,                                                                  \
				 parentIndexRelid,                                                                 \
				 parentConstraintId,                                                               \
				 relFileNumber,                                                                    \
				 indexInfo,                                                                        \
				 indexColNames,                                                                    \
				 accessMethodId,                                                                   \
				 tableSpaceId,                                                                     \
				 collationIds,                                                                     \
				 opclassIds,                                                                       \
				 opclassOptions,                                                                   \
				 coloptions,                                                                       \
				 stattargets,                                                                      \
				 reloptions,                                                                       \
				 flags,                                                                            \
				 constr_flags,                                                                     \
				 allow_system_table_mods,                                                          \
				 is_internal,                                                                      \
				 constraintId)
#endif

#if PG17_LT
/*
 * Overflow-aware comparison functions to be used in qsort. Introduced in PG
 * 17 and included here for older PG versions.
 */
static inline int
pg_cmp_u32(uint32 a, uint32 b)
{
	return (a > b) - (a < b);
}

#endif

#if PG16_LT
/*
 * Similarly, wrappers around labs()/llabs() matching our int64.
 *
 * Introduced on PG16:
 * https://github.com/postgres/postgres/commit/357cfefb09115292cfb98d504199e6df8201c957
 */
#ifdef HAVE_LONG_INT_64
#define i64abs(i) labs(i)
#else
#define i64abs(i) llabs(i)
#endif
#endif

/*
 * PG18 adds IndexScanInstrumentation parameter to index_beginscan
 * https://github.com/postgres/postgres/commit/0fbceae8
 */
#if PG18_LT
#define index_beginscan_compat(heapRelation,                                                       \
							   indexRelation,                                                      \
							   snapshot,                                                           \
							   instrument,                                                         \
							   nkeys,                                                              \
							   norderbys)                                                          \
	index_beginscan(heapRelation, indexRelation, snapshot, nkeys, norderbys)
#else
#define index_beginscan_compat(heapRelation,                                                       \
							   indexRelation,                                                      \
							   snapshot,                                                           \
							   instrument,                                                         \
							   nkeys,                                                              \
							   norderbys)                                                          \
	index_beginscan(heapRelation, indexRelation, snapshot, instrument, nkeys, norderbys)
#endif

#if PG16_LT
#define make_range_compat(typcache, lower, upper, empty, escontext)                                \
	make_range(typcache, lower, upper, empty)
#else
#define make_range_compat(typcache, lower, upper, empty, escontext)                                \
	make_range(typcache, lower, upper, empty, escontext)
#endif

/* Copied from PG17. We can remove it once we deprecate older versions. */
#if PG17_LT
static inline void
initReadOnlyStringInfo(StringInfo str, char *data, int len)
{
	str->data = data;
	str->len = len;
	str->maxlen = 0; /* read-only */
	str->cursor = 0;
}
#endif

/*
 * PG18 renames ri_ConstraintExprs to ri_CheckConstraintExprs
 * Add macros so we can use the new naming for older versions.
 * https://github.com/postgres/postgres/commit/9a9ead11
 */
#if PG18_LT
#define ri_CheckConstraintExprs ri_ConstraintExprs
#endif

/*
 * PG18 renames ec_derives to ec_derives_list
 * Add macros so we can use the new naming for older versions.
 * https://github.com/postgres/postgres/commit/88f55bc9
 */
#if PG18_LT
#define ec_derives_list ec_derives
#endif

/* PG18 introduces new CompareType for ordering operations
 * Add macros so we can use the new naming for older versions.
 * https://github.com/postgres/postgres/commit/8123e91f
 */
#if PG18_LT
#define CompareType int16
#define COMPARE_LT BTLessStrategyNumber
#define COMPARE_GT BTGreaterStrategyNumber
#define pk_cmptype pk_strategy
#endif

/* PG18 adds is_merge_delete param to ExecBR{Delete|Update}Triggers function.
 * This has been backported to 17.6 but with a new name (ExecBR{Delete|Update}TriggersNew)j
 * Add compat function to cover 3 versions (pre 17.6, 17.6 - 18, post 18)
 * https://github.com/postgres/postgres/commit/5022ff25
 */
#if PG_VERSION_NUM < 170006
#define ExecBRDeleteTriggersCompat(estate,                                                         \
								   epqstate,                                                       \
								   relinfo,                                                        \
								   tupleid,                                                        \
								   fdw_trigtuple,                                                  \
								   epqslot,                                                        \
								   tmresult,                                                       \
								   tmfd,                                                           \
								   is_merge_delete)                                                \
	ExecBRDeleteTriggers(estate, epqstate, relinfo, tupleid, fdw_trigtuple, epqslot, tmresult, tmfd)
#define ExecBRUpdateTriggersCompat(estate,                                                         \
								   epqstate,                                                       \
								   relinfo,                                                        \
								   tupleid,                                                        \
								   fdw_trigtuple,                                                  \
								   epqslot,                                                        \
								   tmresult,                                                       \
								   tmfd,                                                           \
								   is_merge_delete)                                                \
	ExecBRUpdateTriggers(estate, epqstate, relinfo, tupleid, fdw_trigtuple, epqslot, tmresult, tmfd)
#endif

#if PG_VERSION_NUM >= 170006 && PG_VERSION_NUM < 180000
#define ExecBRDeleteTriggersCompat(estate,                                                         \
								   epqstate,                                                       \
								   relinfo,                                                        \
								   tupleid,                                                        \
								   fdw_trigtuple,                                                  \
								   epqslot,                                                        \
								   tmresult,                                                       \
								   tmfd,                                                           \
								   is_merge_delete)                                                \
	ExecBRDeleteTriggersNew(estate,                                                                \
							epqstate,                                                              \
							relinfo,                                                               \
							tupleid,                                                               \
							fdw_trigtuple,                                                         \
							epqslot,                                                               \
							tmresult,                                                              \
							tmfd,                                                                  \
							is_merge_delete)
#define ExecBRUpdateTriggersCompat(estate,                                                         \
								   epqstate,                                                       \
								   relinfo,                                                        \
								   tupleid,                                                        \
								   fdw_trigtuple,                                                  \
								   epqslot,                                                        \
								   tmresult,                                                       \
								   tmfd,                                                           \
								   is_merge_delete)                                                \
	ExecBRUpdateTriggersNew(estate,                                                                \
							epqstate,                                                              \
							relinfo,                                                               \
							tupleid,                                                               \
							fdw_trigtuple,                                                         \
							epqslot,                                                               \
							tmresult,                                                              \
							tmfd,                                                                  \
							is_merge_delete)
#endif
#if PG18_GE
#define ExecBRDeleteTriggersCompat(estate,                                                         \
								   epqstate,                                                       \
								   relinfo,                                                        \
								   tupleid,                                                        \
								   fdw_trigtuple,                                                  \
								   epqslot,                                                        \
								   tmresult,                                                       \
								   tmfd,                                                           \
								   is_merge_delete)                                                \
	ExecBRDeleteTriggers(estate,                                                                   \
						 epqstate,                                                                 \
						 relinfo,                                                                  \
						 tupleid,                                                                  \
						 fdw_trigtuple,                                                            \
						 epqslot,                                                                  \
						 tmresult,                                                                 \
						 tmfd,                                                                     \
						 is_merge_delete)
#define ExecBRUpdateTriggersCompat(estate,                                                         \
								   epqstate,                                                       \
								   relinfo,                                                        \
								   tupleid,                                                        \
								   fdw_trigtuple,                                                  \
								   epqslot,                                                        \
								   tmresult,                                                       \
								   tmfd,                                                           \
								   is_merge_delete)                                                \
	ExecBRUpdateTriggers(estate,                                                                   \
						 epqstate,                                                                 \
						 relinfo,                                                                  \
						 tupleid,                                                                  \
						 fdw_trigtuple,                                                            \
						 epqslot,                                                                  \
						 tmresult,                                                                 \
						 tmfd,                                                                     \
						 is_merge_delete)
#endif
