/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/htup_details.h>
#include <access/htup.h>
#include <access/multixact.h>
#include <access/visibilitymap.h>
#include <access/xact.h>
#include <catalog/indexing.h>
#include <catalog/pg_class.h>
#include <catalog/pg_inherits.h>
#include <catalog/pg_namespace.h>
#include <catalog/pg_operator.h>
#include <catalog/pg_type.h>
#include <commands/vacuum.h>
#include <fmgr.h>
#include <funcapi.h>
#include <miscadmin.h>
#include <storage/lmgr.h>
#include <storage/lockdefs.h>
#include <utils/array.h>
#include <utils/builtins.h>
#include <utils/jsonb.h>
#include <utils/lsyscache.h>
#include <utils/palloc.h>
#include <utils/syscache.h>

#include "compat/compat.h"
#include "chunk.h"
#include "chunk_api.h"
#include "errors.h"
#include "error_utils.h"
#include "hypercube.h"
#include "hypertable_cache.h"
#include "utils.h"

#include "ts_catalog/array_utils.h"
#include "ts_catalog/catalog.h"

/*
 * These values come from the pg_type table.
 */
#define FLOAT4_TYPELEN sizeof(float4)
#define FLOAT4_TYPEBYVAL true
#define FLOAT4_TYPEALIGN 'i'
#define CSTRING_TYPELEN (-2)
#define CSTRING_TYPEBYVAL false
#define CSTRING_TYPEALIGN 'c'
#define INT4_TYPELEN sizeof(int32)
#define INT4_TYPEBYVAL true
#define INT4_TYPEALIGN 'i'
#define OID_TYPELEN sizeof(Oid)
#define OID_TYPEBYVAL true
#define OID_TYPEALIGN 'i'
#define CSTRING_ARY_TYPELEN (-1)

/*
 * Convert a hypercube to a JSONB value.
 *
 * For instance, a two-dimensional hypercube, with dimensions "time" and
 * "device", might look as follows:
 *
 * {"time": [1514419200000000, 1515024000000000],
 *  "device": [-9223372036854775808, 1073741823]}
 */
static JsonbValue *
hypercube_to_jsonb_value(Hypercube *hc, Hyperspace *hs, JsonbParseState **ps)
{
	int i;

	Assert(hs->num_dimensions == hc->num_slices);

	pushJsonbValue(ps, WJB_BEGIN_OBJECT, NULL);

	for (i = 0; i < hc->num_slices; i++)
	{
		JsonbValue k, v;
		char *dim_name = NameStr(hs->dimensions[i].fd.column_name);
		Datum range_start =
			DirectFunctionCall1(int8_numeric, Int64GetDatum(hc->slices[i]->fd.range_start));
		Datum range_end =
			DirectFunctionCall1(int8_numeric, Int64GetDatum(hc->slices[i]->fd.range_end));

		Assert(hs->dimensions[i].fd.id == hc->slices[i]->fd.dimension_id);

		k.type = jbvString;
		k.val.string.len = strlen(dim_name);
		k.val.string.val = dim_name;

		pushJsonbValue(ps, WJB_KEY, &k);
		pushJsonbValue(ps, WJB_BEGIN_ARRAY, NULL);

		v.type = jbvNumeric;
		v.val.numeric = DatumGetNumeric(range_start);
		pushJsonbValue(ps, WJB_ELEM, &v);
		v.val.numeric = DatumGetNumeric(range_end);
		pushJsonbValue(ps, WJB_ELEM, &v);

		pushJsonbValue(ps, WJB_END_ARRAY, NULL);
	}

	return pushJsonbValue(ps, WJB_END_OBJECT, NULL);
}

/*
 * Create a hypercube from a JSONB object.
 *
 * Takes a JSONB object with a hypercube's dimensional constraints and outputs
 * a Hypercube. The JSONB is the same format as output by
 * hypercube_to_jsonb_value() above, i.e.:
 *
 * {"time": [1514419200000000, 1515024000000000],
 *  "device": [-9223372036854775808, 1073741823]}
 */
static Hypercube *
hypercube_from_jsonb(Jsonb *json, const Hyperspace *hs, const char **parse_error)
{
	JsonbIterator *it;
	JsonbIteratorToken type;
	JsonbValue v;
	Hypercube *hc = NULL;
	const char *err = NULL;

	it = JsonbIteratorInit(&json->root);

	type = JsonbIteratorNext(&it, &v, false);

	if (type != WJB_BEGIN_OBJECT)
	{
		err = "invalid JSON format";
		goto out_err;
	}

	if (v.val.object.nPairs != hs->num_dimensions)
	{
		err = "invalid number of hypercube dimensions";
		goto out_err;
	}

	hc = ts_hypercube_alloc(hs->num_dimensions);

	while ((type = JsonbIteratorNext(&it, &v, false)))
	{
		int i;
		const Dimension *dim;
		int64 range[2];
		const char *name;

		if (type == WJB_END_OBJECT)
			break;

		if (type != WJB_KEY)
		{
			err = "invalid JSON format";
			goto out_err;
		}

		name = pnstrdup(v.val.string.val, v.val.string.len);
		dim = ts_hyperspace_get_dimension_by_name(hs, DIMENSION_TYPE_ANY, name);

		if (NULL == dim)
		{
			err = psprintf("dimension \"%s\" does not exist in hypertable", name);
			goto out_err;
		}

		type = JsonbIteratorNext(&it, &v, false);

		if (type != WJB_BEGIN_ARRAY)
		{
			err = "invalid JSON format";
			goto out_err;
		}

		if (v.val.array.nElems != 2)
		{
			err = psprintf("unexpected number of dimensional bounds for dimension \"%s\"", name);
			goto out_err;
		}

		for (i = 0; i < 2; i++)
		{
			type = JsonbIteratorNext(&it, &v, false);

			if (type != WJB_ELEM)
			{
				err = "invalid JSON format";
				goto out_err;
			}

			if (v.type != jbvNumeric)
			{
				err = psprintf("constraint for dimension \"%s\" is not numeric", name);
				goto out_err;
			}

			range[i] =
				DatumGetInt64(DirectFunctionCall1(numeric_int8, NumericGetDatum(v.val.numeric)));
		}

		type = JsonbIteratorNext(&it, &v, false);

		if (type != WJB_END_ARRAY)
		{
			err = "invalid JSON format";
			goto out_err;
		}

		ts_hypercube_add_slice_from_range(hc, dim->fd.id, range[0], range[1]);
	}

out_err:
	if (NULL != parse_error)
		*parse_error = err;

	if (NULL != err)
		return NULL;

	return hc;
}

enum Anum_create_chunk
{
	Anum_create_chunk_id = 1,
	Anum_create_chunk_hypertable_id,
	Anum_create_chunk_schema_name,
	Anum_create_chunk_table_name,
	Anum_create_chunk_relkind,
	Anum_create_chunk_slices,
	Anum_create_chunk_created,
	_Anum_create_chunk_max,
};

#define Natts_create_chunk (_Anum_create_chunk_max - 1)

static HeapTuple
chunk_form_tuple(Chunk *chunk, Hypertable *ht, TupleDesc tupdesc, bool created)
{
	Datum values[Natts_create_chunk];
	bool nulls[Natts_create_chunk] = { false };
	JsonbParseState *ps = NULL;
	JsonbValue *jv = hypercube_to_jsonb_value(chunk->cube, ht->space, &ps);

	if (NULL == jv)
		return NULL;

	values[AttrNumberGetAttrOffset(Anum_create_chunk_id)] = Int32GetDatum(chunk->fd.id);
	values[AttrNumberGetAttrOffset(Anum_create_chunk_hypertable_id)] =
		Int32GetDatum(chunk->fd.hypertable_id);
	values[AttrNumberGetAttrOffset(Anum_create_chunk_schema_name)] =
		NameGetDatum(&chunk->fd.schema_name);
	values[AttrNumberGetAttrOffset(Anum_create_chunk_table_name)] =
		NameGetDatum(&chunk->fd.table_name);
	values[AttrNumberGetAttrOffset(Anum_create_chunk_relkind)] = CharGetDatum(chunk->relkind);
	values[AttrNumberGetAttrOffset(Anum_create_chunk_slices)] =
		JsonbPGetDatum(JsonbValueToJsonb(jv));
	values[AttrNumberGetAttrOffset(Anum_create_chunk_created)] = BoolGetDatum(created);

	return heap_form_tuple(tupdesc, values, nulls);
}

Datum
chunk_show(PG_FUNCTION_ARGS)
{
	Oid chunk_relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, true);
	Cache *hcache = ts_hypertable_cache_pin();
	Hypertable *ht =
		ts_hypertable_cache_get_entry(hcache, chunk->hypertable_relid, CACHE_FLAG_NONE);
	TupleDesc tupdesc;
	HeapTuple tuple;

	Assert(NULL != chunk);
	Assert(NULL != ht);

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	/*
	 * We use the create_chunk tuple for show_chunk, because they only differ
	 * in the created column at the end. That column will not be included here
	 * since it is not part of the tuple descriptor.
	 */
	tuple = chunk_form_tuple(chunk, ht, tupdesc, false);

	ts_cache_release(hcache);

	if (NULL == tuple)
		ereport(ERROR,
				(errcode(ERRCODE_TS_INTERNAL_ERROR), errmsg("could not create tuple from chunk")));

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

static void
check_privileges_for_creating_chunk(Oid hyper_relid)
{
	AclResult acl_result;

	acl_result = pg_class_aclcheck(hyper_relid, GetUserId(), ACL_INSERT);
	if (acl_result != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for table \"%s\"", get_rel_name(hyper_relid)),
				 errdetail("Insert privileges required on \"%s\" to create chunks.",
						   get_rel_name(hyper_relid))));
}

static Hypercube *
get_hypercube_from_slices(Jsonb *slices, const Hypertable *ht)
{
	Hypercube *hc;
	const char *parse_err;

	hc = hypercube_from_jsonb(slices, ht->space, &parse_err);

	if (hc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid hypercube for hypertable \"%s\"",
						get_rel_name(ht->main_table_relid)),
				 errdetail("%s", parse_err)));

	return hc;
}

/*
 * Create a chunk and its metadata.
 *
 * This function will create a chunk, either from an existing table or by
 * creating a new table. If chunk_table_relid is InvalidOid, the chunk table
 * will be created, otherwise the table referenced by the relid will be
 * used. The chunk will be associated with the hypertable given by
 * hypertable_relid.
 */
Datum
chunk_create(PG_FUNCTION_ARGS)
{
	Oid hypertable_relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	Jsonb *slices = PG_ARGISNULL(1) ? NULL : PG_GETARG_JSONB_P(1);
	const char *schema_name = PG_ARGISNULL(2) ? NULL : PG_GETARG_CSTRING(2);
	const char *table_name = PG_ARGISNULL(3) ? NULL : PG_GETARG_CSTRING(3);
	Oid chunk_table_relid = PG_ARGISNULL(4) ? InvalidOid : PG_GETARG_OID(4);
	Cache *hcache = ts_hypertable_cache_pin();
	Hypertable *ht = ts_hypertable_cache_get_entry(hcache, hypertable_relid, CACHE_FLAG_NONE);
	Hypercube *hc;
	Chunk *chunk;
	TupleDesc tupdesc;
	HeapTuple tuple;
	bool created;

	Assert(NULL != ht);
	Assert(OidIsValid(ht->main_table_relid));
	check_privileges_for_creating_chunk(hypertable_relid);

	if (NULL == slices)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid slices")));

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	hc = get_hypercube_from_slices(slices, ht);
	Assert(NULL != hc);
	chunk = ts_chunk_find_or_create_without_cuts(ht,
												 hc,
												 schema_name,
												 table_name,
												 chunk_table_relid,
												 &created);
	Assert(NULL != chunk);

	tuple = chunk_form_tuple(chunk, ht, tupdesc, created);

	ts_cache_release(hcache);

	if (NULL == tuple)
		ereport(ERROR,
				(errcode(ERRCODE_TS_INTERNAL_ERROR), errmsg("could not create tuple from chunk")));

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

#define CREATE_CHUNK_FUNCTION_NAME "create_chunk"
#define CREATE_CHUNK_NUM_ARGS 5
#define CHUNK_CREATE_STMT                                                                          \
	"SELECT * FROM " FUNCTIONS_SCHEMA_NAME "." CREATE_CHUNK_FUNCTION_NAME "($1, $2, $3, $4, $5)"

#define ESTIMATE_JSON_STR_SIZE(num_dims) (60 * (num_dims))

enum Anum_chunk_relstats
{
	Anum_chunk_relstats_chunk_id = 1,
	Anum_chunk_relstats_hypertable_id,
	Anum_chunk_relstats_num_pages,
	Anum_chunk_relstats_num_tuples,
	Anum_chunk_relstats_num_allvisible,
	_Anum_chunk_relstats_max,
};

/*
 * Construct a tuple for the get_chunk_relstats SQL function.
 */
static HeapTuple
chunk_get_single_stats_tuple(Chunk *chunk, TupleDesc tupdesc)
{
	HeapTuple ctup;
	Form_pg_class pgcform;
	Datum values[_Anum_chunk_relstats_max];
	bool nulls[_Anum_chunk_relstats_max] = { false };
	Datum reltuples;

	ctup = SearchSysCache1(RELOID, ObjectIdGetDatum(chunk->table_id));

	if (!HeapTupleIsValid(ctup))
		elog(ERROR,
			 "pg_class entry for chunk \"%s.%s\" not found",
			 NameStr(chunk->fd.schema_name),
			 NameStr(chunk->fd.table_name));

	pgcform = (Form_pg_class) GETSTRUCT(ctup);

	values[AttrNumberGetAttrOffset(Anum_chunk_relstats_chunk_id)] = Int32GetDatum(chunk->fd.id);
	values[AttrNumberGetAttrOffset(Anum_chunk_relstats_hypertable_id)] =
		Int32GetDatum(chunk->fd.hypertable_id);
	values[AttrNumberGetAttrOffset(Anum_chunk_relstats_num_pages)] =
		Int32GetDatum(pgcform->relpages);
	reltuples = Float4GetDatum(pgcform->reltuples > 0 ? pgcform->reltuples : 0);
	values[AttrNumberGetAttrOffset(Anum_chunk_relstats_num_tuples)] = reltuples;
	values[AttrNumberGetAttrOffset(Anum_chunk_relstats_num_allvisible)] =
		Int32GetDatum(pgcform->relallvisible);

	ReleaseSysCache(ctup);

	return heap_form_tuple(tupdesc, values, nulls);
}

enum Anum_chunk_colstats
{
	Anum_chunk_colstats_chunk_id = 1,
	Anum_chunk_colstats_hypertable_id,
	Anum_chunk_colstats_column_id,
	Anum_chunk_colstats_nullfrac,
	Anum_chunk_colstats_width,
	Anum_chunk_colstats_distinct,
	Anum_chunk_colstats_slot_kinds,
	Anum_chunk_colstats_slot_op_strings,
	Anum_chunk_colstats_slot_collations,
	Anum_chunk_colstats_slot1_numbers,
	Anum_chunk_colstats_slot2_numbers,
	Anum_chunk_colstats_slot3_numbers,
	Anum_chunk_colstats_slot4_numbers,
	Anum_chunk_colstats_slot5_numbers,
	Anum_chunk_colstats_slot_valtype_strings,
	Anum_chunk_colstats_slot1_values,
	Anum_chunk_colstats_slot2_values,
	Anum_chunk_colstats_slot3_values,
	Anum_chunk_colstats_slot4_values,
	Anum_chunk_colstats_slot5_values,
	_Anum_chunk_colstats_max,
};

/*
 * It's not safe to send OIDs for operations or types between postgres instances for user defined
 * types. Instead convert the operation OID to strings and then look up the local OID for the
 * corresponding names on the other node. A single op OID will need 6 strings {op_name,
 * op_namespace, larg_name, larg_namespace, rarg_name, rarg_namespace}
 */
#define STRINGS_PER_TYPE_OID 2
#define STRINGS_PER_OP_OID 6

enum StringArrayTypeIdx
{
	ENCODED_TYPE_NAME = 0,
	ENCODED_TYPE_NAMESPACE
};

enum OpArrayTypeIdx
{
	ENCODED_OP_NAME = 0,
	ENCODED_OP_NAMESPACE,
	ENCODED_OP_LARG_NAME,
	ENCODED_OP_LARG_NAMESPACE,
	ENCODED_OP_RARG_NAME,
	ENCODED_OP_RARG_NAMESPACE,
};

#define LargSubarrayForOpArray(op_string_array) (&(op_string_array)[ENCODED_OP_LARG_NAME])
#define RargSubarrayForOpArray(op_string_array) (&(op_string_array)[ENCODED_OP_RARG_NAME])

static void
convert_type_oid_to_strings(Oid type_id, Datum *result_strings)
{
	Form_pg_type type;
	Form_pg_namespace namespace;
	HeapTuple namespace_tuple;
	HeapTuple type_tuple;

	type_tuple = SearchSysCache1(TYPEOID, type_id);
	Assert(HeapTupleIsValid(type_tuple));
	type = (Form_pg_type) GETSTRUCT(type_tuple);
	result_strings[ENCODED_TYPE_NAME] = PointerGetDatum(pstrdup(NameStr(type->typname)));

	namespace_tuple = SearchSysCache1(NAMESPACEOID, type->typnamespace);
	Assert(HeapTupleIsValid(namespace_tuple));
	namespace = (Form_pg_namespace) GETSTRUCT(namespace_tuple);
	result_strings[ENCODED_TYPE_NAMESPACE] = PointerGetDatum(pstrdup(NameStr(namespace->nspname)));
	ReleaseSysCache(namespace_tuple);
	ReleaseSysCache(type_tuple);
}

static void
convert_op_oid_to_strings(Oid op_id, Datum *result_strings)
{
	Form_pg_operator operator;
	Form_pg_namespace namespace;
	HeapTuple operator_tuple;
	HeapTuple namespace_tuple;

	operator_tuple = SearchSysCache1(OPEROID, op_id);
	Assert(HeapTupleIsValid(operator_tuple));
	operator=(Form_pg_operator) GETSTRUCT(operator_tuple);
	result_strings[ENCODED_OP_NAME] = PointerGetDatum(pstrdup(NameStr(operator->oprname)));

	namespace_tuple = SearchSysCache1(NAMESPACEOID, operator->oprnamespace);
	Assert(HeapTupleIsValid(namespace_tuple));
	namespace = (Form_pg_namespace) GETSTRUCT(namespace_tuple);
	result_strings[ENCODED_OP_NAMESPACE] = PointerGetDatum(pstrdup(NameStr(namespace->nspname)));
	ReleaseSysCache(namespace_tuple);

	convert_type_oid_to_strings(operator->oprleft, LargSubarrayForOpArray(result_strings));
	convert_type_oid_to_strings(operator->oprright, RargSubarrayForOpArray(result_strings));

	ReleaseSysCache(operator_tuple);
}

static void
collect_colstat_slots(const HeapTuple tuple, const Form_pg_statistic formdata, Datum *values,
					  bool *nulls)
{
	/* Fetching the values and/or numbers fields for a slot requires knowledge of which fields are
	 * present for each kind of stats. Note that this doesn't support custom statistics.
	 */
	static const int statistic_kind_slot_fields[STATISTIC_KIND_BOUNDS_HISTOGRAM + 1] = {
		0,
		ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS, /* MCV */
		ATTSTATSSLOT_VALUES,						/* HISTOGRAM */
		ATTSTATSSLOT_NUMBERS,						/* CORRELATION */
		ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS, /* MCELEM */
		ATTSTATSSLOT_NUMBERS,						/* DECHIST */
		/* ATTSTATSSLOT_VALUES is not always present for all HISTOGRAM operators.. */
		ATTSTATSSLOT_NUMBERS, /* RANGE_LENGTH_HISTOGRAM */
		ATTSTATSSLOT_VALUES	  /* BOUNDS_HISTOGRAM */
	};

	int i;
	Datum slotkind[STATISTIC_NUM_SLOTS];
	Datum op_strings[STRINGS_PER_OP_OID * STATISTIC_NUM_SLOTS];
	Datum slot_collation[STATISTIC_NUM_SLOTS];
	Datum value_type_strings[STRINGS_PER_TYPE_OID * STATISTIC_NUM_SLOTS];
	ArrayType *array;
	int nopstrings = 0;
	int nvalstrings = 0;

	for (i = 0; i < STATISTIC_NUM_SLOTS; i++)
	{
		int16 kind = ((int16 *) (&formdata->stakind1))[i];
		Datum slot_op = ObjectIdGetDatum(((Oid *) &formdata->staop1)[i]);
		AttStatsSlot stat_slot;
		int slot_fields;
		const int numbers_idx = AttrNumberGetAttrOffset(Anum_chunk_colstats_slot1_numbers) + i;
		const int values_idx = AttrNumberGetAttrOffset(Anum_chunk_colstats_slot1_values) + i;

		slot_collation[i] = ObjectIdGetDatum(((Oid *) &formdata->stacoll1)[i]);

		slotkind[i] = ObjectIdGetDatum(kind);

		/*
		 * As per comments in pg_statistic_d.h, "kind" codes from 0 - 99 are reserved
		 * for assignment by the core PostgreSQL project. Beyond that are for PostGIS
		 * and other projects
		 */
#define PG_STATS_KINDS_MAX 99
		if (!OidIsValid(kind) || kind > PG_STATS_KINDS_MAX)
		{
			nulls[numbers_idx] = true;
			nulls[values_idx] = true;
			continue;
		}

		/* slot_op can be invalid for some "kinds" like STATISTIC_KIND_BOUNDS_HISTOGRAM */
		if (OidIsValid(slot_op))
		{
			convert_op_oid_to_strings(slot_op, op_strings + nopstrings);
			nopstrings += STRINGS_PER_OP_OID;
		}

		if (kind > STATISTIC_KIND_BOUNDS_HISTOGRAM)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unable to fetch user defined statistics from data nodes")));

		slot_fields = statistic_kind_slot_fields[kind];

		get_attstatsslot(&stat_slot, tuple, kind, InvalidOid, slot_fields);

		if (slot_fields & ATTSTATSSLOT_NUMBERS)
		{
			Datum *stanumbers = palloc(sizeof(Datum) * stat_slot.nnumbers);
			int j;

			for (j = 0; j < stat_slot.nnumbers; j++)
				stanumbers[j] = Float4GetDatum(stat_slot.numbers[j]);

			array = construct_array(stanumbers,
									stat_slot.nnumbers,
									FLOAT4OID,
									FLOAT4_TYPELEN,
									FLOAT4_TYPEBYVAL,
									FLOAT4_TYPEALIGN);
			values[numbers_idx] = PointerGetDatum(array);
		}
		else
			nulls[numbers_idx] = true;

		if (slot_fields & ATTSTATSSLOT_VALUES)
		{
			Datum *encoded_value_ary = palloc0(sizeof(Datum) * stat_slot.nvalues);
			HeapTuple type_tuple = SearchSysCache1(TYPEOID, stat_slot.valuetype);
			Form_pg_type type;
			int k;

			Assert(HeapTupleIsValid(type_tuple));
			type = (Form_pg_type) GETSTRUCT(type_tuple);
			convert_type_oid_to_strings(stat_slot.valuetype, value_type_strings + nvalstrings);
			nvalstrings += STRINGS_PER_TYPE_OID;

			for (k = 0; k < stat_slot.nvalues; ++k)
				encoded_value_ary[k] = OidFunctionCall1(type->typoutput, stat_slot.values[k]);

			array = construct_array(encoded_value_ary,
									stat_slot.nvalues,
									CSTRINGOID,
									CSTRING_TYPELEN,
									CSTRING_TYPEBYVAL,
									CSTRING_TYPEALIGN);
			values[values_idx] = PointerGetDatum(array);

			ReleaseSysCache(type_tuple);
		}
		else
			nulls[values_idx] = true;

		free_attstatsslot(&stat_slot);
	}

	array = construct_array(slotkind,
							STATISTIC_NUM_SLOTS,
							INT4OID,
							INT4_TYPELEN,
							INT4_TYPEBYVAL,
							INT4_TYPEALIGN);
	values[AttrNumberGetAttrOffset(Anum_chunk_colstats_slot_kinds)] = PointerGetDatum(array);
	array = construct_array(op_strings,
							nopstrings,
							CSTRINGOID,
							CSTRING_TYPELEN,
							CSTRING_TYPEBYVAL,
							CSTRING_TYPEALIGN);
	values[AttrNumberGetAttrOffset(Anum_chunk_colstats_slot_op_strings)] = PointerGetDatum(array);
	array = construct_array(slot_collation,
							STATISTIC_NUM_SLOTS,
							OIDOID,
							OID_TYPELEN,
							OID_TYPEBYVAL,
							OID_TYPEALIGN);
	values[AttrNumberGetAttrOffset(Anum_chunk_colstats_slot_collations)] = PointerGetDatum(array);
	array = construct_array(value_type_strings,
							nvalstrings,
							CSTRINGOID,
							CSTRING_TYPELEN,
							CSTRING_TYPEBYVAL,
							CSTRING_TYPEALIGN);
	values[AttrNumberGetAttrOffset(Anum_chunk_colstats_slot_valtype_strings)] =
		PointerGetDatum(array);
}

/*
 * Construct a tuple for the get_chunk_relstats SQL function.
 */
static HeapTuple
chunk_get_single_colstats_tuple(Chunk *chunk, int column, TupleDesc tupdesc)
{
	HeapTuple ctup;
	Form_pg_statistic pgsform;
	Datum values[_Anum_chunk_colstats_max];
	bool nulls[_Anum_chunk_colstats_max] = { false };
	bool dropped;

	if (DatumGetBool(DirectFunctionCall1(row_security_active, ObjectIdGetDatum(chunk->table_id))))
		return NULL;

	ctup = SearchSysCache2(ATTNUM, ObjectIdGetDatum(chunk->table_id), column);
	if (!HeapTupleIsValid(ctup))
		return NULL;

	dropped = ((Form_pg_attribute) GETSTRUCT(ctup))->attisdropped;
	ReleaseSysCache(ctup);

	if (dropped)
		return NULL;

	if (!DatumGetBool(DirectFunctionCall3(has_column_privilege_id_attnum,
										  ObjectIdGetDatum(chunk->table_id),
										  Int16GetDatum(column),
										  PointerGetDatum(cstring_to_text("SELECT")))))
		return NULL;

	ctup = SearchSysCache3(STATRELATTINH, ObjectIdGetDatum(chunk->table_id), column, false);

	/* pg_statistics will not have an entry for an unanalyzed table */
	if (!HeapTupleIsValid(ctup))
		return NULL;

	pgsform = (Form_pg_statistic) GETSTRUCT(ctup);

	values[AttrNumberGetAttrOffset(Anum_chunk_colstats_chunk_id)] = Int32GetDatum(chunk->fd.id);
	values[AttrNumberGetAttrOffset(Anum_chunk_colstats_hypertable_id)] =
		Int32GetDatum(chunk->fd.hypertable_id);
	values[AttrNumberGetAttrOffset(Anum_chunk_colstats_column_id)] = Int32GetDatum(column);
	values[AttrNumberGetAttrOffset(Anum_chunk_colstats_nullfrac)] =
		Float4GetDatum(pgsform->stanullfrac);
	values[AttrNumberGetAttrOffset(Anum_chunk_colstats_width)] = Int32GetDatum(pgsform->stawidth);
	values[AttrNumberGetAttrOffset(Anum_chunk_colstats_distinct)] =
		Float4GetDatum(pgsform->stadistinct);

	collect_colstat_slots(ctup, pgsform, values, nulls);

	ReleaseSysCache(ctup);

	return heap_form_tuple(tupdesc, values, nulls);
}

/*
 * StatsProcessContext filters out duplicate stats from replica chunks.
 *
 * When processing chunk stats from data nodes, we might receive the same
 * stats from multiple data nodes when native replication is enabled. With the
 * StatsProcessContext we can filter out the duplicates, and ensure we only
 * add the stats once. Without the filtering, we will get errors (e.g., unique
 * violations).
 *
 * We could elide the filtering if we requested stats only for the chunks that
 * are "primary", but that requires the ability to specify the specific remote
 * chunks to retrieve stats for rather than specifying "all chunks" for the
 * given hypertable.
 */
typedef struct ChunkAttKey
{
	Oid chunk_relid;
	Index attnum;
} ChunkAttKey;

typedef struct StatsProcessContext
{
	HTAB *htab;
	MemoryContext per_tuple_mcxt;
} StatsProcessContext;

static void *
chunk_api_generate_relstats_context(List *oids)
{
	return list_copy(oids);
}

static HeapTuple
chunk_api_fetch_next_relstats_tuple(FuncCallContext *funcctx)
{
	List *chunk_oids = (List *) funcctx->user_fctx;
	Oid relid;
	Chunk *chunk;

	if (chunk_oids == NIL)
		return NULL;

	relid = linitial_oid(chunk_oids);
	chunk = ts_chunk_get_by_relid(relid, true);

	return chunk_get_single_stats_tuple(chunk, funcctx->tuple_desc);
}

static void
chunk_api_iterate_relstats_context(FuncCallContext *funcctx)
{
	List *chunk_oids = (List *) funcctx->user_fctx;
	TS_WITH_MEMORY_CONTEXT(funcctx->multi_call_memory_ctx, {
		chunk_oids = list_delete_first(chunk_oids);
		funcctx->user_fctx = chunk_oids;
	});
}

typedef struct ColStatContext
{
	List *chunk_oids;
	int col_id;
	int nattrs;
} ColStatContext;

static void *
chunk_api_generate_colstats_context(List *oids, Oid ht_relid)
{
	ColStatContext *ctx = palloc0(sizeof(ColStatContext));

	ctx->chunk_oids = list_copy(oids);
	ctx->col_id = 1;
	ctx->nattrs = ts_get_relnatts(ht_relid);

	return ctx;
}

static HeapTuple
chunk_api_fetch_next_colstats_tuple(FuncCallContext *funcctx)
{
	ColStatContext *ctx = funcctx->user_fctx;
	HeapTuple tuple = NULL;

	while (tuple == NULL && ctx->chunk_oids != NIL)
	{
		Oid relid = linitial_oid(ctx->chunk_oids);
		Chunk *chunk = ts_chunk_get_by_relid(relid, true);

		tuple = chunk_get_single_colstats_tuple(chunk, ctx->col_id, funcctx->tuple_desc);

		/* This loop looks a bit odd as col_id tracks the postgres column id, which is indexed
		 * starting at 1 */
		while (tuple == NULL && ctx->col_id < ctx->nattrs)
		{
			++ctx->col_id;
			tuple = chunk_get_single_colstats_tuple(chunk, ctx->col_id, funcctx->tuple_desc);
		}

		if (tuple == NULL)
		{
			TS_WITH_MEMORY_CONTEXT(funcctx->multi_call_memory_ctx, {
				ctx->chunk_oids = list_delete_first(ctx->chunk_oids);
				ctx->col_id = 1;
			});
		}
	}

	return tuple;
}

static void
chunk_api_iterate_colstats_context(FuncCallContext *funcctx)
{
	ColStatContext *ctx = funcctx->user_fctx;
	TS_WITH_MEMORY_CONTEXT(funcctx->multi_call_memory_ctx, {
		++ctx->col_id;
		if (AttrNumberGetAttrOffset(ctx->col_id) >= ctx->nattrs)
		{
			ctx->chunk_oids = list_delete_first(ctx->chunk_oids);
			ctx->col_id = 1;
		}
	});
}

#define GET_CHUNK_RELSTATS_NAME "get_chunk_relstats"
#define GET_CHUNK_COLSTATS_NAME "get_chunk_colstats"

/*
 * Get relation stats or column stats for chunks.
 *
 * This function takes a hypertable or chunk as input (regclass). In case of a
 * hypertable, it will get the relstats for all the chunks in the hypertable,
 * otherwise only the given chunk.
 */
static Datum
chunk_api_get_chunk_stats(FunctionCallInfo fcinfo, bool col_stats)
{
	FuncCallContext *funcctx;
	HeapTuple tuple;

	if (SRF_IS_FIRSTCALL())
	{
		Oid relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
		TupleDesc tupdesc;
		Cache *hcache;
		Hypertable *ht;
		List *chunk_oids = NIL;
		Oid ht_relid = InvalidOid;

		if (!OidIsValid(relid))
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid table")));

		ht = ts_hypertable_cache_get_cache_and_entry(relid, CACHE_FLAG_MISSING_OK, &hcache);

		if (NULL == ht)
		{
			Chunk *chunk = ts_chunk_get_by_relid(relid, false);

			if (NULL == chunk)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("must be a hypertable or chunk")));

			chunk_oids = list_make1_oid(chunk->table_id);

			/* We'll need the hypertable if fetching column stats */
			if (col_stats)
				ht = ts_hypertable_get_by_id(chunk->fd.hypertable_id);
		}
		else
		{
			chunk_oids = find_inheritance_children(relid, NoLock);
		}

		if (ht)
			ht_relid = ht->main_table_relid;
		ts_cache_release(hcache);

		funcctx = SRF_FIRSTCALL_INIT();
		TS_WITH_MEMORY_CONTEXT(funcctx->multi_call_memory_ctx, {
			if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("function returning record called in context "
								"that cannot accept type record")));

			/* Save the chunk oid list on the multi-call memory context so that it
			 * survives across multiple calls to this function (until SRF is
			 * done). */
			funcctx->user_fctx = col_stats ?
									 chunk_api_generate_colstats_context(chunk_oids, ht_relid) :
									 chunk_api_generate_relstats_context(chunk_oids);
			funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		});
	}

	funcctx = SRF_PERCALL_SETUP();
	tuple = col_stats ? chunk_api_fetch_next_colstats_tuple(funcctx) :
						chunk_api_fetch_next_relstats_tuple(funcctx);

	if (tuple == NULL)
		SRF_RETURN_DONE(funcctx);

	if (col_stats)
		chunk_api_iterate_colstats_context(funcctx);
	else
		chunk_api_iterate_relstats_context(funcctx);

	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
}

Datum
chunk_api_get_chunk_colstats(PG_FUNCTION_ARGS)
{
	return chunk_api_get_chunk_stats(fcinfo, true);
}

Datum
chunk_api_get_chunk_relstats(PG_FUNCTION_ARGS)
{
	return chunk_api_get_chunk_stats(fcinfo, false);
}

Datum
chunk_create_empty_table(PG_FUNCTION_ARGS)
{
	Oid hypertable_relid;
	Jsonb *slices;
	const char *schema_name;
	const char *table_name;
	Cache *const hcache = ts_hypertable_cache_pin();
	Hypertable *ht;
	Hypercube *hc;
	Oid uid, saved_uid;
	int sec_ctx;

	GETARG_NOTNULL_OID(hypertable_relid, 0, "hypertable");
	GETARG_NOTNULL_NULLABLE(slices, 1, "slices", JSONB_P);
	GETARG_NOTNULL_NULLABLE(schema_name, 2, "chunk schema name", CSTRING);
	GETARG_NOTNULL_NULLABLE(table_name, 3, "chunk table name", CSTRING);

	ht = ts_hypertable_cache_get_entry(hcache, hypertable_relid, CACHE_FLAG_NONE);
	Assert(ht != NULL);

	/*
	 * If the chunk is created in the internal schema, become the catalog
	 * owner, otherwise become the hypertable owner
	 */
	if (strcmp(schema_name, INTERNAL_SCHEMA_NAME) == 0)
		uid = ts_catalog_database_info_get()->owner_uid;
	else
	{
		Relation rel;

		rel = table_open(ht->main_table_relid, AccessShareLock);
		uid = rel->rd_rel->relowner;
		table_close(rel, AccessShareLock);
	}

	GetUserIdAndSecContext(&saved_uid, &sec_ctx);

	if (uid != saved_uid)
		SetUserIdAndSecContext(uid, sec_ctx | SECURITY_LOCAL_USERID_CHANGE);

	hc = get_hypercube_from_slices(slices, ht);
	Assert(NULL != hc);
	ts_chunk_create_only_table(ht, hc, schema_name, table_name);

	ts_cache_release(hcache);

	/* Need to restore security context */
	if (uid != saved_uid)
		SetUserIdAndSecContext(saved_uid, sec_ctx);

	PG_RETURN_BOOL(true);
}
