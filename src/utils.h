/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_UTILS_H
#define TIMESCALEDB_UTILS_H

#include <postgres.h>
#include <access/htup_details.h>
#include <catalog/pg_proc.h>
#include <catalog/namespace.h>
#include <common/int.h>
#include <foreign/foreign.h>
#include <nodes/pathnodes.h>
#include <nodes/extensible.h>
#include <utils/datetime.h>
#include <debug_assert.h>

#include "compat/compat.h"

/*
 * Get the function name in a PG_FUNCTION.
 *
 * The function name is resolved from the function Oid in the functioncall
 * data. However, this information is not present in case of a direct function
 * call, so fall back to the C-function name.
 */
#define TS_FUNCNAME()                                                                              \
	(psprintf("%s()", fcinfo->flinfo ? get_func_name(FC_FN_OID(fcinfo)) : __func__))

#define TS_PREVENT_FUNC_IF_READ_ONLY() PreventCommandIfReadOnly(TS_FUNCNAME())
#define TS_PREVENT_IN_TRANSACTION_BLOCK(toplevel) PreventInTransactionBlock(toplevel, TS_FUNCNAME())

#define MAX(x, y) ((x) > (y) ? x : y)
#define MIN(x, y) ((x) < (y) ? x : y)

/* Use a special pseudo-random field 4 value to avoid conflicting with user-advisory-locks */
#define TS_SET_LOCKTAG_ADVISORY(tag, id1, id2, id3)                                                \
	SET_LOCKTAG_ADVISORY((tag), (id1), (id2), (id3), 29749)

/* find the length of a statically sized array */
#define TS_ARRAY_LEN(array) (sizeof(array) / sizeof(*array))

/* Use condition to check if out of memory */
#define TS_OOM_CHECK(COND, FMT, ...)                                                               \
	do                                                                                             \
	{                                                                                              \
		if (!(COND))                                                                               \
			ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg(FMT, ##__VA_ARGS__)));          \
	} while (0)

extern TSDLLEXPORT bool ts_type_is_int8_binary_compatible(Oid sourcetype);

typedef enum TimevalInfinity
{
	TimevalFinite = 0,
	TimevalNegInfinity = -1,
	TimevalPosInfinity = 1,
} TimevalInfinity;

typedef bool (*proc_filter)(Form_pg_proc form, void *arg);

/*
 * Convert a column value into the internal time representation.
 * cannot store a timestamp earlier than MIN_TIMESTAMP, or greater than
 *    END_TIMESTAMP - TS_EPOCH_DIFF_MICROSECONDS
 * nor dates that cannot be translated to timestamps
 * Will throw an error for that, or other conversion issues.
 */
extern TSDLLEXPORT int64 ts_time_value_to_internal(Datum time_val, Oid type);
extern int64 ts_time_value_to_internal_or_infinite(Datum time_val, Oid type_oid,
												   TimevalInfinity *is_infinite_out);

extern TSDLLEXPORT int64 ts_interval_value_to_internal(Datum time_val, Oid type_oid);

/*
 * Convert a column from the internal time representation into the specified type
 */
extern TSDLLEXPORT Datum ts_internal_to_time_value(int64 value, Oid type);
extern TSDLLEXPORT int64 ts_internal_to_time_int64(int64 value, Oid type);
extern TSDLLEXPORT Datum ts_internal_to_interval_value(int64 value, Oid type);
extern TSDLLEXPORT char *ts_internal_to_time_string(int64 value, Oid type);

/*
 * Return the period in microseconds of the first argument to date_trunc.
 * This is approximate -- to be used for planning;
 */
extern int64 ts_date_trunc_interval_period_approx(text *units);
/*
 * Return the interval period in microseconds.
 * This is approximate -- to be used for planning;
 */
extern TSDLLEXPORT int64 ts_get_interval_period_approx(Interval *interval);

extern TSDLLEXPORT Oid ts_inheritance_parent_relid(Oid relid);

extern Oid ts_lookup_proc_filtered(const char *schema, const char *funcname, Oid *rettype,
								   proc_filter filter, void *filter_arg);
extern Oid ts_get_operator(const char *name, Oid namespace, Oid left, Oid right);
extern bool ts_function_types_equal(Oid left[], Oid right[], int nargs);

extern TSDLLEXPORT Oid ts_get_function_oid(const char *funcname, const char *schema_name, int nargs,
										   Oid arg_types[]);

extern TSDLLEXPORT Oid ts_get_cast_func(Oid source, Oid target);

typedef struct Dimension Dimension;

extern TSDLLEXPORT Oid ts_get_integer_now_func(const Dimension *open_dim);
extern TSDLLEXPORT int64 ts_sub_integer_from_now(int64 interval, Oid time_dim_type, Oid now_func);

extern TSDLLEXPORT void *ts_create_struct_from_slot(TupleTableSlot *slot, MemoryContext mctx,
													size_t alloc_size, size_t copy_size);

extern TSDLLEXPORT AppendRelInfo *ts_get_appendrelinfo(PlannerInfo *root, Index rti,
													   bool missing_ok);

#if PG15_GE
extern TSDLLEXPORT Expr *ts_find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel);
#endif

extern TSDLLEXPORT bool ts_has_row_security(Oid relid);

extern TSDLLEXPORT List *ts_get_reloptions(Oid relid);

#define STRUCT_FROM_SLOT(slot, mctx, to_type, form_type)                                           \
	(to_type *) ts_create_struct_from_slot(slot, mctx, sizeof(to_type), sizeof(form_type));

/* note PG10 has_superclass but PG96 does not so use this */
#define is_inheritance_child(relid) (OidIsValid(ts_inheritance_parent_relid((relid))))

#define is_inheritance_parent(relid)                                                               \
	(find_inheritance_children(table_relid, AccessShareLock) != NIL)

#define is_inheritance_table(relid) (is_inheritance_child(relid) || is_inheritance_parent(relid))

static inline int64
int64_min(int64 a, int64 b)
{
	if (a <= b)
		return a;
	return b;
}

static inline int64
int64_saturating_add(int64 a, int64 b)
{
	int64 result;
	bool overflowed = pg_add_s64_overflow(a, b, &result);
	if (overflowed)
		result = a < 0 ? PG_INT64_MIN : PG_INT64_MAX;
	return result;
}

static inline int64
int64_saturating_sub(int64 a, int64 b)
{
	int64 result;
	bool overflowed = pg_sub_s64_overflow(a, b, &result);
	if (overflowed)
		result = b < 0 ? PG_INT64_MAX : PG_INT64_MIN;
	return result;
}

static inline bool
ts_flags_are_set_32(uint32 bitmap, uint32 flags)
{
	return (bitmap & flags) == flags;
}

static inline pg_nodiscard uint32
ts_set_flags_32(uint32 bitmap, uint32 flags)
{
	return bitmap | flags;
}

static inline uint32
ts_clear_flags_32(uint32 bitmap, uint32 flags)
{
	return bitmap & ~flags;
}

/**
 * Try to register a custom scan method.
 *
 * When registering a custom scan node, it might be called multiple times when
 * different databases have different versions of the extension installed, so
 * this function can be used to try to register a custom scan method but not
 * fail if it has already been registered.
 */
static inline void
TryRegisterCustomScanMethods(const CustomScanMethods *methods)
{
	if (!GetCustomScanMethods(methods->CustomName, true))
		RegisterCustomScanMethods(methods);
}

typedef struct RelationSize
{
	int64 total_size;
	int64 heap_size;
	int64 toast_size;
	int64 index_size;
} RelationSize;

extern TSDLLEXPORT RelationSize ts_relation_size_impl(Oid relid);

extern TSDLLEXPORT const char *ts_get_node_name(Node *node);
extern TSDLLEXPORT int ts_get_relnatts(Oid relid);
extern TSDLLEXPORT void ts_alter_table_with_event_trigger(Oid relid, Node *cmd, List *cmds,
														  bool recurse);
extern TSDLLEXPORT void ts_copy_relation_acl(const Oid source_relid, const Oid target_relid,
											 const Oid owner_id);
extern TSDLLEXPORT bool ts_data_node_is_available_by_server(const ForeignServer *server);
extern TSDLLEXPORT bool ts_data_node_is_available(const char *node_name);

extern TSDLLEXPORT AttrNumber ts_map_attno(Oid src_rel, Oid dst_rel, AttrNumber attno);

/*
 * Return Oid for a schema-qualified relation.
 */
static inline Oid
ts_get_relation_relid(char const *schema_name, char const *relation_name, bool return_invalid)
{
	Oid schema_oid = get_namespace_oid(schema_name, true);

	if (OidIsValid(schema_oid))
	{
		Oid rel_oid = get_relname_relid(relation_name, schema_oid);

		if (!return_invalid)
			Ensure(OidIsValid(rel_oid), "relation \"%s.%s\" not found", schema_name, relation_name);

		return rel_oid;
	}
	else
	{
		if (!return_invalid)
			Ensure(OidIsValid(schema_oid),
				   "schema \"%s\" not found (during lookup of relation \"%s.%s\")",
				   schema_name,
				   schema_name,
				   relation_name);

		return InvalidOid;
	}
}

#endif /* TIMESCALEDB_UTILS_H */
