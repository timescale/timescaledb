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
#include <executor/executor.h>
#include <executor/tuptable.h>
#include <nodes/execnodes.h>
#include <nodes/nodes.h>
#include <optimizer/restrictinfo.h>
#include <pgstat.h>
#include <utils/jsonb.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>

#include "export.h"

#define PG_MAJOR_MIN 14

#define is_supported_pg_version_14(version) ((version >= 140000) && (version < 150000))
#define is_supported_pg_version_15(version) ((version >= 150000) && (version < 160000))
#define is_supported_pg_version_16(version) ((version >= 160000) && (version < 170000))

/*
 * PG16 support is a WIP and not complete yet.
 * To compile with PG16, use -DEXPERIMENTAL=ON with cmake.
 */
#define is_supported_pg_version(version)                                                           \
	(is_supported_pg_version_14(version) || is_supported_pg_version_15(version) ||                 \
	 is_supported_pg_version_16(version))

#define PG14 is_supported_pg_version_14(PG_VERSION_NUM)
#define PG15 is_supported_pg_version_15(PG_VERSION_NUM)
#define PG16 is_supported_pg_version_16(PG_VERSION_NUM)

#define PG14_LT (PG_VERSION_NUM < 140000)
#define PG14_GE (PG_VERSION_NUM >= 140000)
#define PG15_LT (PG_VERSION_NUM < 150000)
#define PG15_GE (PG_VERSION_NUM >= 150000)
#define PG16_LT (PG_VERSION_NUM < 160000)
#define PG16_GE (PG_VERSION_NUM >= 160000)

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

/*
 * The number of arguments of pg_md5_hash() has changed in PG 15.
 *
 * https://github.com/postgres/postgres/commit/b69aba74
 */

#if PG15_LT

#include <common/md5.h>
static inline bool
pg_md5_hash_compat(const void *buff, size_t len, char *hexsum, const char **errstr)
{
	*errstr = NULL;
	return pg_md5_hash(buff, len, hexsum);
}

#else

#include <common/md5.h>
#define pg_md5_hash_compat(buff, len, hexsum, errstr) pg_md5_hash(buff, len, hexsum, errstr)

#endif

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

/* find_em_expr_for_rel was in postgres_fdw in PG12 but got
 * moved out of contrib and into core in PG13. PG15 removed
 * the function again from postgres core code so for PG15+
 * we fall back to our own implementation.
 */
#if PG15_GE
#define find_em_expr_for_rel ts_find_em_expr_for_rel
#endif

/*
 * PG15 added additional `force_flush` argument to shm_mq_send().
 *
 * Our _compat() version currently uses force_flush = true on PG15 to preserve
 * the same behaviour on all supported PostgreSQL versions.
 *
 * https://git.postgresql.org/gitweb/?p=postgresql.git;a=commit;h=46846433
 */
#if PG15_GE
#define shm_mq_send_compat(shm_mq_handle, nbytes, data, nowait)                                    \
	shm_mq_send(shm_mq_handle, nbytes, data, nowait, true)
#else
#define shm_mq_send_compat(shm_mq_handle, nbytes, data, nowait)                                    \
	shm_mq_send(shm_mq_handle, nbytes, data, nowait)
#endif

/*
 * The macro FirstBootstrapObjectId was renamed in PG15.
 *
 * https://git.postgresql.org/gitweb/?p=postgresql.git;a=commit;h=a49d0812
 */
#if PG15_GE
#define FirstBootstrapObjectIdCompat FirstUnpinnedObjectId
#else
#define FirstBootstrapObjectIdCompat FirstBootstrapObjectId
#endif

/*
 * The number of arguments of make_new_heap() has changed in PG15. Note that
 * on PostgreSQL <= 14 our _compat() version ignores the NewAccessMethod
 * argument and uses the default access method.
 *
 * https://git.postgresql.org/gitweb/?p=postgresql.git;a=commit;h=b0483263
 */
#if PG15_GE
#define make_new_heap_compat(tableOid, tableSpace, NewAccessMethod, relpersistence, ExclusiveLock) \
	make_new_heap(tableOid, tableSpace, NewAccessMethod, relpersistence, ExclusiveLock)
#else
#define make_new_heap_compat(tableOid, tableSpace, _ignored, relpersistence, ExclusiveLock)        \
	make_new_heap(tableOid, tableSpace, relpersistence, ExclusiveLock)
#endif

/*
 * PostgreSQL 15 removed "utils/int8.h" header and change the "scanint8"
 * function to "pg_strtoint64" in "utils/builtins.h".
 *
 * https://github.com/postgres/postgres/commit/cfc7191dfea330dd7a71e940d59de78129bb6175
 */
#if PG15_LT
#include <utils/int8.h>
static inline int64
pg_strtoint64(const char *str)
{
	int64 result;
	scanint8(str, false, &result);

	return result;
}
#else
#include <utils/builtins.h>
#endif

/*
 * PG 15 removes "recheck" argument from check_index_is_clusterable
 *
 * https://github.com/postgres/postgres/commit/b940918d
 */
#if PG15_GE
#define check_index_is_clusterable_compat(rel, indexOid, lock)                                     \
	check_index_is_clusterable(rel, indexOid, lock)
#else
#define check_index_is_clusterable_compat(rel, indexOid, lock)                                     \
	check_index_is_clusterable(rel, indexOid, true, lock)
#endif

/*
 * PG15 consolidate VACUUM xid cutoff logic.
 *
 * https://github.com/postgres/postgres/commit/efa4a946
 */
#if PG15_LT
#define vacuum_set_xid_limits_compat(rel,                                                          \
									 freeze_min_age,                                               \
									 freeze_table_age,                                             \
									 multixact_freeze_min_age,                                     \
									 multixact_freeze_table_age,                                   \
									 oldestXmin,                                                   \
									 freezeLimit,                                                  \
									 multiXactCutoff)                                              \
	vacuum_set_xid_limits(rel,                                                                     \
						  freeze_min_age,                                                          \
						  freeze_table_age,                                                        \
						  multixact_freeze_min_age,                                                \
						  multixact_freeze_table_age,                                              \
						  oldestXmin,                                                              \
						  freezeLimit,                                                             \
						  NULL,                                                                    \
						  multiXactCutoff,                                                         \
						  NULL)
#elif PG16_LT
#define vacuum_set_xid_limits_compat(rel,                                                          \
									 freeze_min_age,                                               \
									 freeze_table_age,                                             \
									 multixact_freeze_min_age,                                     \
									 multixact_freeze_table_age,                                   \
									 oldestXmin,                                                   \
									 freezeLimit,                                                  \
									 multiXactCutoff)                                              \
	do                                                                                             \
	{                                                                                              \
		MultiXactId oldestMxact;                                                                   \
		vacuum_set_xid_limits(rel,                                                                 \
							  freeze_min_age,                                                      \
							  freeze_table_age,                                                    \
							  multixact_freeze_min_age,                                            \
							  multixact_freeze_table_age,                                          \
							  oldestXmin,                                                          \
							  &oldestMxact,                                                        \
							  freezeLimit,                                                         \
							  multiXactCutoff);                                                    \
	} while (0)
#else
#define vacuum_set_xid_limits_compat(rel,                                                          \
									 freezeMinAge,                                                 \
									 freezeTableAge,                                               \
									 multixactFreezeMinAge,                                        \
									 multixactFreezeTableAge,                                      \
									 oldestXmin,                                                   \
									 freezeLimit,                                                  \
									 multiXactCutoff)                                              \
	do                                                                                             \
	{                                                                                              \
		struct VacuumCutoffs cutoffs;                                                              \
		/* vacuum_get_cutoffs uses only the *_age members of the VacuumParams object */            \
		VacuumParams params = { .freeze_min_age = freezeMinAge,                                    \
								.freeze_table_age = freezeTableAge,                                \
								.multixact_freeze_min_age = multixactFreezeMinAge,                 \
								.multixact_freeze_table_age = multixactFreezeTableAge };           \
		vacuum_get_cutoffs(rel, &params, &cutoffs);                                                \
		*(oldestXmin) = cutoffs.OldestXmin;                                                        \
		*(freezeLimit) = cutoffs.FreezeLimit;                                                      \
		*(multiXactCutoff) = cutoffs.MultiXactCutoff;                                              \
	} while (0)
#endif

/*
 * PG15 updated the signatures of ExecARUpdateTriggers and ExecARDeleteTriggers while
 * fixing foreign key handling during cross-partition updates
 *
 * https://github.com/postgres/postgres/commit/ba9a7e39217
 */
#if PG15_LT
#define ExecARUpdateTriggersCompat(estate,                                                         \
								   resultRelInfo,                                                  \
								   src_partinfo,                                                   \
								   dst_partinfo,                                                   \
								   tupleid,                                                        \
								   oldtuple,                                                       \
								   inewslot,                                                       \
								   recheckIndexes,                                                 \
								   transtition_capture,                                            \
								   is_crosspart_update)                                            \
	ExecARUpdateTriggers(estate,                                                                   \
						 resultRelInfo,                                                            \
						 tupleid,                                                                  \
						 oldtuple,                                                                 \
						 inewslot,                                                                 \
						 recheckIndexes,                                                           \
						 transtition_capture)
#define ExecARDeleteTriggersCompat(estate,                                                         \
								   resultRelInfo,                                                  \
								   tupleid,                                                        \
								   oldtuple,                                                       \
								   ar_delete_trig_tcs,                                             \
								   is_crosspart_update)                                            \
	ExecARDeleteTriggers(estate, resultRelInfo, tupleid, oldtuple, ar_delete_trig_tcs)
#else
#define ExecARUpdateTriggersCompat(estate,                                                         \
								   resultRelInfo,                                                  \
								   src_partinfo,                                                   \
								   dst_partinfo,                                                   \
								   tupleid,                                                        \
								   oldtuple,                                                       \
								   inewslot,                                                       \
								   recheckIndexes,                                                 \
								   transtition_capture,                                            \
								   is_crosspart_update)                                            \
	ExecARUpdateTriggers(estate,                                                                   \
						 resultRelInfo,                                                            \
						 src_partinfo,                                                             \
						 dst_partinfo,                                                             \
						 tupleid,                                                                  \
						 oldtuple,                                                                 \
						 inewslot,                                                                 \
						 recheckIndexes,                                                           \
						 transtition_capture,                                                      \
						 is_crosspart_update)
#define ExecARDeleteTriggersCompat(estate,                                                         \
								   resultRelInfo,                                                  \
								   tupleid,                                                        \
								   oldtuple,                                                       \
								   ar_delete_trig_tcs,                                             \
								   is_crosspart_update)                                            \
	ExecARDeleteTriggers(estate,                                                                   \
						 resultRelInfo,                                                            \
						 tupleid,                                                                  \
						 oldtuple,                                                                 \
						 ar_delete_trig_tcs,                                                       \
						 is_crosspart_update)
#endif

/*
 * PG15 adds new argument TM_FailureData to ExecBRUpdateTriggers
 * as a part of adding support for Merge
 * https://github.com/postgres/postgres/commit/9321c79c
 *
 * PG16 adds TMResult argument to ExecBRUpdateTriggers
 * https://github.com/postgres/postgres/commit/7103ebb7
 */
#if PG15_LT
#define ExecBRUpdateTriggersCompat(estate,                                                         \
								   epqstate,                                                       \
								   resultRelInfo,                                                  \
								   tupleid,                                                        \
								   oldtuple,                                                       \
								   slot,                                                           \
								   result,                                                         \
								   tmfdp)                                                          \
	ExecBRUpdateTriggers(estate, epqstate, resultRelInfo, tupleid, oldtuple, slot)
#elif PG16_LT
#define ExecBRUpdateTriggersCompat(estate,                                                         \
								   epqstate,                                                       \
								   resultRelInfo,                                                  \
								   tupleid,                                                        \
								   oldtuple,                                                       \
								   slot,                                                           \
								   result,                                                         \
								   tmfdp)                                                          \
	ExecBRUpdateTriggers(estate, epqstate, resultRelInfo, tupleid, oldtuple, slot, tmfdp)
#else
#define ExecBRUpdateTriggersCompat(estate,                                                         \
								   epqstate,                                                       \
								   resultRelInfo,                                                  \
								   tupleid,                                                        \
								   oldtuple,                                                       \
								   slot,                                                           \
								   result,                                                         \
								   tmfdp)                                                          \
	ExecBRUpdateTriggers(estate, epqstate, resultRelInfo, tupleid, oldtuple, slot, result, tmfdp)
#endif

/*
 * PG16 adds TMResult argument to ExecBRDeleteTriggers
 * https://github.com/postgres/postgres/commit/9321c79c
 */
#if PG16_LT
#define ExecBRDeleteTriggersCompat(estate,                                                         \
								   epqstate,                                                       \
								   relinfo,                                                        \
								   tupleid,                                                        \
								   fdw_trigtuple,                                                  \
								   epqslot,                                                        \
								   tmresult,                                                       \
								   tmfd)                                                           \
	ExecBRDeleteTriggers(estate, epqstate, relinfo, tupleid, fdw_trigtuple, epqslot)
#else
#define ExecBRDeleteTriggersCompat(estate,                                                         \
								   epqstate,                                                       \
								   relinfo,                                                        \
								   tupleid,                                                        \
								   fdw_trigtuple,                                                  \
								   epqslot,                                                        \
								   tmresult,                                                       \
								   tmfd)                                                           \
	ExecBRDeleteTriggers(estate, epqstate, relinfo, tupleid, fdw_trigtuple, epqslot, tmresult, tmfd)
#endif

#if PG14 && PG_VERSION_NUM < 140007
#include <storage/smgr.h>
/*
 * RelationGetSmgr
 *		Returns smgr file handle for a relation, opening it if needed.
 *
 * Very little code is authorized to touch rel->rd_smgr directly.  Instead
 * use this function to fetch its value.
 *
 * Note: since a relcache flush can cause the file handle to be closed again,
 * it's unwise to hold onto the pointer returned by this function for any
 * long period.  Recommended practice is to just re-execute RelationGetSmgr
 * each time you need to access the SMgrRelation.  It's quite cheap in
 * comparison to whatever an smgr function is going to do.
 *
 * This has been backported but is not available in all minor versions so
 * we backport ourselves for those versions.
 *
 */
static inline SMgrRelation
RelationGetSmgr(Relation rel)
{
	if (unlikely(rel->rd_smgr == NULL))
		smgrsetowner(&(rel->rd_smgr), smgropen(rel->rd_node, rel->rd_backend));
	return rel->rd_smgr;
}
#endif

#if PG15_GE
#define GenerationContextCreateCompat(parent, name, blockSize)                                     \
	GenerationContextCreate(parent, name, 0, blockSize, blockSize)
#else
#define GenerationContextCreateCompat(parent, name, blockSize)                                     \
	GenerationContextCreate(parent, name, blockSize)
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

/*
 * PG15 refactored elog.c functions and exposed error_severity
 * but previous versions don't have it exposed, so imported it
 * from Postgres source code.
 *
 * https://github.com/postgres/postgres/commit/ac7c80758a7
 */
#if PG15_LT
/*
 * error_severity --- get string representing elevel
 *
 * The string is not localized here, but we mark the strings for translation
 * so that callers can invoke _() on the result.
 *
 * Imported from src/backend/utils/error/elog.c
 */
static inline const char *
error_severity(int elevel)
{
	const char *prefix;

	switch (elevel)
	{
		case DEBUG1:
		case DEBUG2:
		case DEBUG3:
		case DEBUG4:
		case DEBUG5:
			prefix = gettext_noop("DEBUG");
			break;
		case LOG:
		case LOG_SERVER_ONLY:
			prefix = gettext_noop("LOG");
			break;
		case INFO:
			prefix = gettext_noop("INFO");
			break;
		case NOTICE:
			prefix = gettext_noop("NOTICE");
			break;
		case WARNING:
		case WARNING_CLIENT_ONLY:
			prefix = gettext_noop("WARNING");
			break;
		case ERROR:
			prefix = gettext_noop("ERROR");
			break;
		case FATAL:
			prefix = gettext_noop("FATAL");
			break;
		case PANIC:
			prefix = gettext_noop("PANIC");
			break;
		default:
			prefix = "???";
			break;
	}

	return prefix;
}
#endif
