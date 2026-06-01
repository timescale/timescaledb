/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>

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

/*
 * Prevent building against upstream versions that had ABI breaking change (15.9, 16.5, 17.1)
 * that was reverted in the following release.
 */

#define is_supported_pg_version_16(version) ((version >= 160006) && (version < 170000))
#define is_supported_pg_version_17(version) ((version >= 170002) && (version < 180000))
#define is_supported_pg_version_18(version) ((version >= 180000) && (version < 190000))
#define is_supported_pg_version_19(version) ((version >= 190000) && (version < 200000))

/*
 * To compile with an unsupported version, use -DEXPERIMENTAL=ON with cmake.
 * (Useful when testing with unreleased versions)
 */
#define is_supported_pg_version(version)                                                           \
	(is_supported_pg_version_16(version) || is_supported_pg_version_17(version) ||                 \
	 is_supported_pg_version_18(version) || is_supported_pg_version_19(version))

#define PG16 is_supported_pg_version_16(PG_VERSION_NUM)
#define PG17 is_supported_pg_version_17(PG_VERSION_NUM)
#define PG18 is_supported_pg_version_18(PG_VERSION_NUM)
#define PG19 is_supported_pg_version_19(PG_VERSION_NUM)

#define PG16_LT (PG_VERSION_NUM < 160000)
#define PG16_GE (PG_VERSION_NUM >= 160000)
#define PG17_LT (PG_VERSION_NUM < 170000)
#define PG17_GE (PG_VERSION_NUM >= 170000)
#define PG18_LT (PG_VERSION_NUM < 180000)
#define PG18_GE (PG_VERSION_NUM >= 180000)
#define PG19_LT (PG_VERSION_NUM < 190000)
#define PG19_GE (PG_VERSION_NUM >= 190000)

#if PG19_GE
#include <commands/repack.h>
#else
#include <commands/cluster.h>
#endif

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

#if PG19_LT
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
		{
			verbose = defGetBoolean(opt);
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unrecognized CLUSTER option \"%s\"", opt->defname),
					 parser_errposition(NULL, opt->location)));
		}
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
		{
			verbose = defGetBoolean(opt);
		}
		else if (strcmp(opt->defname, "concurrently") == 0)
		{
			concurrently = defGetBoolean(opt);
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unrecognized REINDEX option \"%s\"", opt->defname),
					 parser_errposition(NULL, opt->location)));
		}
	}
	return (verbose ? REINDEXOPT_VERBOSE : 0) | (concurrently ? REINDEXOPT_CONCURRENTLY : 0);
}

/*
 * define some list macros for convenience
 */
#define lfifth(l) lfirst(list_nth_cell(l, 4))
#define lfifth_int(l) lfirst_int(list_nth_cell(l, 4))

/*
 * PG16 adds a new parameter to DefineIndex, total_parts, that takes
 * in the total number of direct and indirect partitions of the relation.
 *
 * https://github.com/postgres/postgres/commit/27f5c712
 */
#if PG19_LT
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

/*
 * murmurhash64 was added to common/hashfn.h in PG17 (954e43564d9).
 */
static inline uint64
murmurhash64(uint64 data)
{
	uint64 h = data;

	h ^= h >> 33;
	h *= 0xff51afd7ed558ccd;
	h ^= h >> 33;
	h *= 0xc4ceb9fe1a85ec53;
	h ^= h >> 33;

	return h;
}
#endif

#if PG17_LT

/* This macro was renamed in PG17, see 414f6c0fb79a */
#define WAIT_EVENT_MESSAGE_QUEUE_INTERNAL WAIT_EVENT_MQ_INTERNAL

/* 'flush' argument was added in 173b56f1ef59 */
#define LogLogicalMessageCompat(prefix, message, size, transactional, flush)                       \
	LogLogicalMessage(prefix, message, size, transactional)

/* 'stmt' argument was added in f21848de2013 */
#define reindex_relation_compat(stmt, relid, flags, params) reindex_relation(relid, flags, params)

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
/* 'mergeActions' argument was added in 5f2e179bd31e */
#define CheckValidResultRelCompat(resultRelInfo, operation, onConflictAction, mergeActions)        \
	CheckValidResultRel(resultRelInfo, operation)
#elif PG18_LT
#define CheckValidResultRelCompat(resultRelInfo, operation, onConflictAction, mergeActions)        \
	CheckValidResultRel(resultRelInfo, operation, mergeActions)
#else
#define CheckValidResultRelCompat(resultRelInfo, operation, onConflictAction, mergeActions)        \
	CheckValidResultRel(resultRelInfo, operation, onConflictAction, mergeActions)
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

#if PG17_LT
/*
 * PG17 added the 'orstronger' parameter to LockHeldByMe.  On older versions
 * we use LockOrStrongerHeldByMe when orstronger is true.
 */
static inline bool
LockHeldByMeCompat(const LOCKTAG *locktag, LOCKMODE lockmode, bool orstronger)
{
	if (orstronger)
	{
		return LockOrStrongerHeldByMe(locktag, lockmode);
	}
	return LockHeldByMe(locktag, lockmode);
}
#else
#define LockHeldByMeCompat(locktag, lockmode, orstronger)                                          \
	LockHeldByMe(locktag, lockmode, orstronger)
#endif

/* PG18 introduced PG_MODULE_MAGIC_EXT macro
   https://github.com/postgres/postgres/commit/9324c8c580655800331b0582b770e88c01b7a5c4 */
#ifdef PG_MODULE_MAGIC_EXT
#define TS_MODULE_MAGIC(extname)                                                                   \
	PG_MODULE_MAGIC_EXT(.name = extname, .version = TIMESCALEDB_VERSION_MOD)
#else
#define TS_MODULE_MAGIC(extname) PG_MODULE_MAGIC
#endif

#ifdef PG17_LT
#define binaryheap_size(h) ((h)->bh_size)
#endif
