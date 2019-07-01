/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <utils/jsonb.h>
#include <utils/lsyscache.h>
#include <utils/builtins.h>
#include <catalog/pg_type.h>
#include <fmgr.h>
#include <funcapi.h>
#include <miscadmin.h>

#include <catalog.h>
#include <compat.h>
#include <chunk.h>
#include <chunk_data_node.h>
#include <errors.h>
#include <hypercube.h>
#include <hypertable.h>
#include <hypertable_cache.h>

#include "remote/async.h"
#include "remote/dist_txn.h"
#include "remote/stmt_params.h"
#include "chunk_api.h"

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
hypercube_from_jsonb(Jsonb *json, Hyperspace *hs, const char **parse_error)
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
		Dimension *dim;
		DimensionSlice *slice;
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

		slice = ts_dimension_slice_create(dim->fd.id, range[0], range[1]);
		ts_hypercube_add_slice(hc, slice);
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
	Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, 2, true);
	Cache *hcache = ts_hypertable_cache_pin();
	Hypertable *ht = ts_hypertable_cache_get_entry(hcache, chunk->hypertable_relid);
	TupleDesc tupdesc;
	HeapTuple tuple;

	if (NULL == chunk)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid chunk")));

	if (NULL == ht)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("invalid hypertable")));

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

Datum
chunk_create(PG_FUNCTION_ARGS)
{
	Oid hypertable_relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	Jsonb *slices = PG_ARGISNULL(1) ? NULL : PG_GETARG_JSONB_P(1);
	const char *schema_name = PG_ARGISNULL(2) ? NULL : PG_GETARG_CSTRING(2);
	const char *table_name = PG_ARGISNULL(3) ? NULL : PG_GETARG_CSTRING(3);
	Cache *hcache = ts_hypertable_cache_pin();
	Hypertable *ht = ts_hypertable_cache_get_entry(hcache, hypertable_relid);
	Hypercube *hc;
	Chunk *chunk;
	TupleDesc tupdesc;
	HeapTuple tuple;
	bool created;
	const char *parse_err;

	if (NULL == ht)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid hypertable")));

	if (NULL == slices)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid slices")));

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	hc = hypercube_from_jsonb(slices, ht->space, &parse_err);

	if (NULL == hc)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid hypercube for hypertable \"%s\"", get_rel_name(hypertable_relid)),
				 errdetail("%s", parse_err)));

	chunk = ts_chunk_find_or_create_without_cuts(ht, hc, schema_name, table_name, &created);

	Assert(NULL != chunk);

	tuple = chunk_form_tuple(chunk, ht, tupdesc, created);

	ts_cache_release(hcache);

	if (NULL == tuple)
		ereport(ERROR,
				(errcode(ERRCODE_TS_INTERNAL_ERROR), errmsg("could not create tuple from chunk")));

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

#define CREATE_CHUNK_FUNCTION_NAME "create_chunk"
#define CHUNK_CREATE_STMT                                                                          \
	"SELECT * FROM " INTERNAL_SCHEMA_NAME "." CREATE_CHUNK_FUNCTION_NAME "($1, $2, $3, $4)"

#define ESTIMATE_JSON_STR_SIZE(num_dims) (60 * (num_dims))

static Oid create_chunk_argtypes[4] = { REGCLASSOID, JSONBOID, NAMEOID, NAMEOID };

/*
 * Fill in / get the TupleDesc for the result type of the create_chunk()
 * function.
 */
static void
get_create_chunk_result_type(TupleDesc *tupdesc)
{
	Oid funcoid = ts_get_function_oid(CREATE_CHUNK_FUNCTION_NAME,
									  INTERNAL_SCHEMA_NAME,
									  4,
									  create_chunk_argtypes);

	if (get_func_result_type(funcoid, NULL, tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));
}

static void
get_result_datums(Datum *values, bool *nulls, unsigned int numvals, AttInMetadata *attinmeta,
				  PGresult *res)
{
	unsigned int i;

	memset(nulls, 0, sizeof(bool) * numvals);

	for (i = 0; i < numvals; i++)
	{
		if (PQgetisnull(res, 0, i))
			nulls[i] = true;
		else
			values[i] = InputFunctionCall(&attinmeta->attinfuncs[i],
										  PQgetvalue(res, 0, i),
										  attinmeta->attioparams[i],
										  attinmeta->atttypmods[i]);
	}
}

/*
 * Create a replica of a chunk on all its assigned servers.
 */
void
chunk_api_create_on_servers(Chunk *chunk, Hypertable *ht)
{
	AsyncRequestSet *reqset = async_request_set_create();
	JsonbParseState *ps = NULL;
	JsonbValue *jv = hypercube_to_jsonb_value(chunk->cube, ht->space, &ps);
	Jsonb *hcjson = JsonbValueToJsonb(jv);
	const char *params[4] = {
		quote_qualified_identifier(NameStr(ht->fd.schema_name), NameStr(ht->fd.table_name)),
		JsonbToCString(NULL, &hcjson->root, ESTIMATE_JSON_STR_SIZE(ht->space->num_dimensions)),
		NameStr(chunk->fd.schema_name),
		NameStr(chunk->fd.table_name),
	};
	AsyncResponseResult *res;
	ListCell *lc;
	TupleDesc tupdesc;
	AttInMetadata *attinmeta;

	get_create_chunk_result_type(&tupdesc);
	attinmeta = TupleDescGetAttInMetadata(tupdesc);

	foreach (lc, chunk->servers)
	{
		ChunkServer *cs = lfirst(lc);
		UserMapping *um = GetUserMapping(GetUserId(), cs->foreign_server_oid);
		TSConnection *conn = remote_dist_txn_get_connection(um, REMOTE_TXN_NO_PREP_STMT);
		AsyncRequest *req;

		req = async_request_send_with_params(conn,
											 CHUNK_CREATE_STMT,
											 stmt_params_create_from_values(params, 4),
											 FORMAT_TEXT);

		async_request_attach_user_data(req, cs);
		async_request_set_add(reqset, req);
	}

	while ((res = async_request_set_wait_ok_result(reqset)) != NULL)
	{
		PGresult *pgres = async_response_result_get_pg_result(res);
		ChunkServer *cs = async_response_result_get_user_data(res);
		Datum values[tupdesc->natts];
		bool nulls[tupdesc->natts];
		const char *schema_name, *table_name;
		bool created;

		get_result_datums(values, nulls, tupdesc->natts, attinmeta, pgres);

		created = DatumGetBool(values[AttrNumberGetAttrOffset(Anum_create_chunk_created)]);

		/*
		 * Sanity check the result. Use error rather than an assert since this
		 * is the result of a remote call to a server that could potentially
		 * run a different version of the remote function than we'd expect.
		 */
		if (!created)
			elog(ERROR, "chunk creation failed on server \"%s\"", NameStr(cs->fd.server_name));

		if (nulls[AttrNumberGetAttrOffset(Anum_create_chunk_id)] ||
			nulls[AttrNumberGetAttrOffset(Anum_create_chunk_schema_name)] ||
			nulls[AttrNumberGetAttrOffset(Anum_create_chunk_table_name)])
			elog(ERROR, "unexpected chunk creation result on remote server");

		schema_name =
			DatumGetCString(values[AttrNumberGetAttrOffset(Anum_create_chunk_schema_name)]);
		table_name = DatumGetCString(values[AttrNumberGetAttrOffset(Anum_create_chunk_table_name)]);

		if (namestrcmp(&chunk->fd.schema_name, schema_name) != 0 ||
			namestrcmp(&chunk->fd.table_name, table_name) != 0)
			elog(ERROR, "remote chunk has mismatching schema or table name");

		cs->fd.server_chunk_id =
			DatumGetInt32(values[AttrNumberGetAttrOffset(Anum_create_chunk_id)]);
	}
}
