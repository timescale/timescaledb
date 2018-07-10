#ifndef TIMESCALEDB_COMPAT_H
#define TIMESCALEDB_COMPAT_H

#define is_supported_pg_version_96(version) ((version >= 90603) && (version < 100000))
#define is_supported_pg_version_10(version) ((version >= 100002) && (version < 110000))
#define is_supported_pg_version(version) (is_supported_pg_version_96(version) || is_supported_pg_version_10(version))

#define PG96 is_supported_pg_version_96(PG_VERSION_NUM)
#define PG10 is_supported_pg_version_10(PG_VERSION_NUM)

#if PG10

#define ExecARInsertTriggersCompat(estate, result_rel_info, tuple, recheck_indexes) \
	ExecARInsertTriggers(estate, result_rel_info, tuple, recheck_indexes, NULL)
#define ExecASInsertTriggersCompat(estate, result_rel_info) \
	ExecASInsertTriggers(estate, result_rel_info, NULL)
#define InitResultRelInfoCompat(result_rel_info, result_rel_desc, result_rel_index, instrument_options) \
	InitResultRelInfo(result_rel_info, result_rel_desc, result_rel_index, NULL, instrument_options)
#define CheckValidResultRelCompat(relinfo, operation)	\
	CheckValidResultRel(relinfo, operation)
#define ParseFuncOrColumnCompat(pstate, funcname, fargs, fn, location) \
	ParseFuncOrColumn(pstate, funcname, fargs, (pstate)->p_last_srf, fn, location)
#define make_op_compat(pstate, opname, ltree, rtree, location)	\
	make_op(pstate, opname, ltree, rtree, (pstate)->p_last_srf, location)
#define get_projection_info_slot_compat(pinfo) \
	(pinfo->pi_state.resultslot)
#define map_variable_attnos_compat(returning_clauses, varno, sublevels_up, map, map_size, rowtype, found_whole_row) \
	map_variable_attnos(returning_clauses, varno, sublevels_up, map, map_size, rowtype, found_whole_row);
#define ExecBuildProjectionInfoCompat(tl, exprContext, slot, parent, inputdesc) \
	 ExecBuildProjectionInfo(tl, exprContext, slot, parent, inputdesc)

#elif PG96

#define ExecARInsertTriggersCompat(estate, result_rel_info, tuple, recheck_indexes) \
	ExecARInsertTriggers(estate, result_rel_info, tuple, recheck_indexes)
#define ExecASInsertTriggersCompat(estate, result_rel_info) \
	ExecASInsertTriggers(estate, result_rel_info)
#define InitResultRelInfoCompat(result_rel_info, result_rel_desc, result_rel_index, instrument_options) \
	InitResultRelInfo(result_rel_info, result_rel_desc, result_rel_index, instrument_options)
#define CheckValidResultRelCompat(relinfo, operation) \
	CheckValidResultRel((relinfo)->ri_RelationDesc, operation)
#define ParseFuncOrColumnCompat(pstate, funcname, fargs, fn, location) \
	ParseFuncOrColumn(pstate, funcname, fargs, fn, location)
#define make_op_compat(pstate, opname, ltree, rtree, location)	\
	make_op(pstate, opname, ltree, rtree, location)
#define get_projection_info_slot_compat(pinfo) \
	(pinfo->pi_slot)
#define map_variable_attnos_compat(expr, varno, sublevels_up, map, map_size, rowtype, found_whole_row) \
	map_variable_attnos(expr, varno, sublevels_up, map, map_size, found_whole_row)
#define ExecBuildProjectionInfoCompat(tl, exprContext, slot, parent, inputdesc) \
	 ExecBuildProjectionInfo((List *)ExecInitExpr((Expr *) tl, NULL), exprContext, slot, inputdesc)

#else

#error "Unsupported PostgreSQL version"

#endif							/* PG_VERSION_NUM */

#define TS_FUNCTION_INFO_V1(fn) \
	PGDLLEXPORT Datum fn(PG_FUNCTION_ARGS); \
	PG_FUNCTION_INFO_V1(fn)

#endif							/* TIMESCALEDB_COMPAT_H */
