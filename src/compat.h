#ifndef TIMESCALEDB_COMPAT_H
#define TIMESCALEDB_COMPAT_H

#define PG96 ((PG_VERSION_NUM >= 90600) && (PG_VERSION_NUM < 100000))
#define PG10 ((PG_VERSION_NUM >= 100000) && (PG_VERSION_NUM < 110000))

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

#else

#error "Unsupported PostgreSQL version"

#endif							/* PG_VERSION_NUM */

#define TS_FUNCTION_INFO_V1(fn) \
	PGDLLEXPORT Datum fn(PG_FUNCTION_ARGS); \
	PG_FUNCTION_INFO_V1(fn)

#endif							/* TIMESCALEDB_COMPAT_H */
