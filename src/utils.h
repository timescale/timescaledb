/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include <access/htup_details.h>
#include <access/tupdesc.h>
#include <catalog/namespace.h>
#include <catalog/pg_proc.h>
#include <common/int.h>
#include <debug_assert.h>
#include <foreign/foreign.h>
#include <nodes/extensible.h>
#include <nodes/pathnodes.h>
#include <utils/builtins.h>
#include <utils/datetime.h>
#include <utils/jsonb.h>

#include "compat/compat.h"

/* Convenience macro to execute a simple or complex statement inside a memory
 * context */
#define TS_WITH_MEMORY_CONTEXT(MCXT, STMT)                                                         \
	do                                                                                             \
	{                                                                                              \
		MemoryContext _oldmcxt = MemoryContextSwitchTo((MCXT));                                    \
		do                                                                                         \
			STMT while (0);                                                                        \
		MemoryContextSwitchTo(_oldmcxt);                                                           \
	} while (0)

/*
 * Macro for debug messages that should *only* be present in debug builds but
 * which should be removed in release builds. This is typically used for
 * debug builds for development purposes.
 *
 * Note that some debug messages might be relevant to deploy in release build
 * for debugging production systems. This macro is *not* for those cases.
 */
#ifdef TS_DEBUG
#define TS_DEBUG_LOG(FMT, ...) elog(DEBUG2, "%s - " FMT, __func__, ##__VA_ARGS__)
#else
#define TS_DEBUG_LOG(FMT, ...)
#endif

#ifdef TS_DEBUG

static inline const char *
yes_no(bool value)
{
	return value ? "yes" : "no";
}

/* Convert datum to string using the output function. */
static inline const char *
datum_as_string(Oid typid, Datum value, bool is_null)
{
	Oid typoutput;
	bool typIsVarlena;

	if (is_null)
		return "<NULL>";

	getTypeOutputInfo(typid, &typoutput, &typIsVarlena);
	return OidOutputFunctionCall(typoutput, value);
}

static inline const char *
slot_as_string(TupleTableSlot *slot)
{
	StringInfoData info;
	initStringInfo(&info);
	appendStringInfoString(&info, "{");
	for (int i = 0; i < slot->tts_tupleDescriptor->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(slot->tts_tupleDescriptor, i);

		if (att->attisdropped)
			continue;
		appendStringInfo(&info,
						 "%s: %s",
						 NameStr(att->attname),
						 datum_as_string(att->atttypid, slot->tts_values[i], slot->tts_isnull[i]));
		if (i + 1 < slot->tts_tupleDescriptor->natts)
			appendStringInfoString(&info, ", ");
	}
	appendStringInfoString(&info, "}");
	return info.data;
}

#endif /* TS_DEBUG */

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

extern TSDLLEXPORT bool ts_type_is_int8_binary_compatible(Oid sourcetype);

typedef bool (*proc_filter)(Form_pg_proc form, void *arg);

/*
 * Convert a column value into the internal time representation.
 * cannot store a timestamp earlier than MIN_TIMESTAMP, or greater than
 *    END_TIMESTAMP - TS_EPOCH_DIFF_MICROSECONDS
 * nor dates that cannot be translated to timestamps
 * Will throw an error for that, or other conversion issues.
 */
extern TSDLLEXPORT int64 ts_time_value_to_internal(Datum time_val, Oid type);
extern int64 ts_time_value_to_internal_or_infinite(Datum time_val, Oid type_oid);

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

extern TSDLLEXPORT Oid ts_get_integer_now_func(const Dimension *open_dim, bool fail_if_not_found);
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

#define INIT_NULL_DATUM                                                                            \
	{                                                                                              \
		.value = 0, .isnull = true                                                                 \
	}

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

extern TSDLLEXPORT bool ts_relation_has_tuples(Relation rel);
extern TSDLLEXPORT bool ts_table_has_tuples(Oid table_relid, LOCKMODE lockmode);

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

struct Hypertable;

void replace_now_mock_walker(PlannerInfo *root, Node *clause, Oid funcid);

extern TSDLLEXPORT HeapTuple ts_heap_form_tuple(TupleDesc tupleDescriptor, NullableDatum *datums);

static inline void
ts_datum_set_text_from_cstring(const AttrNumber attno, NullableDatum *datums, const char *value)
{
	if (value != NULL)
	{
		datums[AttrNumberGetAttrOffset(attno)].value = PointerGetDatum(cstring_to_text(value));
		datums[AttrNumberGetAttrOffset(attno)].isnull = false;
	}
	else
		datums[AttrNumberGetAttrOffset(attno)].isnull = true;
}

static inline void
ts_datum_set_bool(const AttrNumber attno, NullableDatum *datums, const bool value)
{
	datums[AttrNumberGetAttrOffset(attno)].value = BoolGetDatum(value);
	datums[AttrNumberGetAttrOffset(attno)].isnull = false;
}

static inline void
ts_datum_set_int32(const AttrNumber attno, NullableDatum *datums, const int32 value,
				   const bool isnull)
{
	datums[AttrNumberGetAttrOffset(attno)].value = Int32GetDatum(value);
	datums[AttrNumberGetAttrOffset(attno)].isnull = isnull;
}

static inline void
ts_datum_set_int64(const AttrNumber attno, NullableDatum *datums, const int64 value,
				   const bool isnull)
{
	datums[AttrNumberGetAttrOffset(attno)].value = Int64GetDatum(value);
	datums[AttrNumberGetAttrOffset(attno)].isnull = isnull;
}

static inline void
ts_datum_set_timestamptz(const AttrNumber attno, NullableDatum *datums, const TimestampTz value,
						 const bool isnull)
{
	datums[AttrNumberGetAttrOffset(attno)].value = TimestampTzGetDatum(value);
	datums[AttrNumberGetAttrOffset(attno)].isnull = isnull;
}

static inline void
ts_datum_set_jsonb(const AttrNumber attno, NullableDatum *datums, const Jsonb *value)
{
	if (value != NULL)
	{
		datums[AttrNumberGetAttrOffset(attno)].value = JsonbPGetDatum(value);
		datums[AttrNumberGetAttrOffset(attno)].isnull = false;
	}
	else
		datums[AttrNumberGetAttrOffset(attno)].isnull = true;
}

static inline void
ts_datum_set_objectid(const AttrNumber attno, NullableDatum *datums, const Oid value)
{
	if (OidIsValid(value))
	{
		datums[AttrNumberGetAttrOffset(attno)].value = ObjectIdGetDatum(value);
		datums[AttrNumberGetAttrOffset(attno)].isnull = false;
	}
	else
		datums[AttrNumberGetAttrOffset(attno)].isnull = true;
}

extern TSDLLEXPORT void ts_get_rel_info_by_name(const char *relnamespace, const char *relname,
												Oid *relid, Oid *amoid, char *relkind);
extern TSDLLEXPORT void ts_get_rel_info(Oid relid, Oid *amoid, char *relkind);
extern TSDLLEXPORT Oid ts_get_rel_am(Oid relid);
extern TSDLLEXPORT bool ts_is_hypercore_am(Oid amoid);
extern TSDLLEXPORT Jsonb *ts_errdata_to_jsonb(ErrorData *edata, Name proc_schema, Name proc_name);
