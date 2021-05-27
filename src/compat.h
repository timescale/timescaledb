/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_COMPAT_H
#define TIMESCALEDB_COMPAT_H

#include <postgres.h>
#include <commands/explain.h>
#include <commands/trigger.h>
#include <executor/executor.h>
#include <executor/tuptable.h>
#include <nodes/execnodes.h>
#include <nodes/nodes.h>
#include <pgstat.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>

#include "export.h"

#define TS_PREVENT_FUNC_IF_READ_ONLY()                                                             \
	(PreventCommandIfReadOnly(psprintf("%s()", get_func_name(FC_FN_OID(fcinfo)))))

#define is_supported_pg_version_12(version) ((version >= 120000) && (version < 130000))
#define is_supported_pg_version_13(version) ((version >= 130002) && (version < 140000))

#define is_supported_pg_version(version)                                                           \
	(is_supported_pg_version_12(version) || is_supported_pg_version_13(version))

#define PG12 is_supported_pg_version_12(PG_VERSION_NUM)
#define PG13 is_supported_pg_version_13(PG_VERSION_NUM)

#define PG13_LT (PG_VERSION_NUM < 130000)
#define PG13_GE (PG_VERSION_NUM >= 130000)

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

#if PG13_GE
#define ExecComputeStoredGeneratedCompat(estate, slot, cmd_type)                                   \
	ExecComputeStoredGenerated(estate, slot, cmd_type)
#else
#define ExecComputeStoredGeneratedCompat(estate, slot, cmd_type)                                   \
	ExecComputeStoredGenerated(estate, slot)
#endif

#define TTSOpsVirtualP (&TTSOpsVirtual)
#define TTSOpsHeapTupleP (&TTSOpsHeapTuple)
#define TTSOpsMinimalTupleP (&TTSOpsMinimalTuple)
#define TTSOpsBufferHeapTupleP (&TTSOpsBufferHeapTuple)

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

#include <commands/vacuum.h>
#include <commands/defrem.h>

static inline int
get_vacuum_options(const VacuumStmt *stmt)
{
	/* In PG12, the vacuum options is a list of DefElems and require
	 * parsing. Here we only parse the options we might be interested in since
	 * PostgreSQL itself will parse the options fully when it executes the
	 * vacuum. */
	ListCell *lc;
	bool analyze = false;
	bool verbose = false;

	foreach (lc, stmt->options)
	{
		DefElem *opt = (DefElem *) lfirst(lc);

		/* Parse common options for VACUUM and ANALYZE */
		if (strcmp(opt->defname, "verbose") == 0)
			verbose = defGetBoolean(opt);
		/* Parse options available on VACUUM */
		else if (strcmp(opt->defname, "analyze") == 0)
			analyze = defGetBoolean(opt);
	}

	return (stmt->is_vacuumcmd ? VACOPT_VACUUM : VACOPT_ANALYZE) | (verbose ? VACOPT_VERBOSE : 0) |
		   (analyze ? VACOPT_ANALYZE : 0);
}

/* PG13 added a dstlen parameter to pg_b64_decode and pg_b64_encode */
#if PG13_LT
#define pg_b64_encode_compat(src, srclen, dst, dstlen) pg_b64_encode((src), (srclen), (dst))
#define pg_b64_decode_compat(src, srclen, dst, dstlen) pg_b64_decode((src), (srclen), (dst))
#else
#define pg_b64_encode_compat(src, srclen, dst, dstlen)                                             \
	pg_b64_encode((src), (srclen), (dst), (dstlen))
#define pg_b64_decode_compat(src, srclen, dst, dstlen)                                             \
	pg_b64_decode((src), (srclen), (dst), (dstlen))
#endif

/* PG13 changes the List implementation from a linked list to an array
 * while most of the API functions did not change a few them have slightly
 * different signature in PG13, additionally the list_make5 functions
 * got removed. */
#if PG13_LT
#define lnext_compat(l, lc) lnext((lc))
#define list_delete_cell_compat(l, lc, prev) list_delete_cell((l), (lc), (prev))
#define for_each_cell_compat(cell, list, initcell) for_each_cell ((cell), (initcell))
#else
#define lnext_compat(l, lc) lnext((l), (lc))
#define list_delete_cell_compat(l, lc, prev) list_delete_cell((l), (lc))
#define list_make5(x1, x2, x3, x4, x5) lappend(list_make4(x1, x2, x3, x4), x5)
#define list_make5_oid(x1, x2, x3, x4, x5) lappend_oid(list_make4_oid(x1, x2, x3, x4), x5)
#define list_make5_int(x1, x2, x3, x4, x5) lappend_int(list_make4_int(x1, x2, x3, x4), x5)
#define for_each_cell_compat(cell, list, initcell) for_each_cell (cell, list, initcell)
#endif

/* PG13 removes the natts parameter from map_variable_attnos */
#if PG13_LT
#define map_variable_attnos_compat(node, varno, sublevels_up, map, natts, rowtype, found_wholerow) \
	map_variable_attnos((node),                                                                    \
						(varno),                                                                   \
						(sublevels_up),                                                            \
						(map),                                                                     \
						(natts),                                                                   \
						(rowtype),                                                                 \
						(found_wholerow))
#else
#define map_variable_attnos_compat(node, varno, sublevels_up, map, natts, rowtype, found_wholerow) \
	map_variable_attnos((node), (varno), (sublevels_up), (map), (rowtype), (found_wholerow))
#endif

#endif /* TIMESCALEDB_COMPAT_H */
