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

#include "chunk_api.h"
#include "chunk.h"
#include "compat/compat.h"
#include "error_utils.h"
#include "errors.h"
#include "hypercube.h"
#include "hypertable_cache.h"
#include "ts_catalog/array_utils.h"
#include "ts_catalog/catalog.h"
#include "utils.h"

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
