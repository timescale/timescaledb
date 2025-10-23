/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>

#include <access/htup.h>
#include <access/htup_details.h>
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
#include <nodes/makefuncs.h>
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
#include "debug_point.h"
#include "error_utils.h"
#include "errors.h"
#include "hypercube.h"
#include "hypertable_cache.h"
#include "ts_catalog/array_utils.h"
#include "ts_catalog/catalog.h"
#include "utils.h"

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

			if (v.type == jbvString)
			{
				if (!IS_TIMESTAMP_TYPE(dim->fd.column_type))
				{
					err =
						psprintf("constraint for dimension \"%s\" can be string only for date time",
								 name);
					goto out_err;
				}

				char *v_str = (char *) palloc(v.val.string.len + 1);
				memcpy(v_str, v.val.string.val, v.val.string.len);
				v_str[v.val.string.len] = '\0';
				range[i] = ts_time_value_from_arg(CStringGetDatum(v_str),
												  InvalidOid,
												  dim->fd.column_type,
												  true);
			}
			else if (v.type == jbvNumeric)
			{
				range[i] = DatumGetInt64(
					DirectFunctionCall1(numeric_int8, NumericGetDatum(v.val.numeric)));
			}
			else
			{
				err = psprintf("constraint for dimension \"%s\" should be either numeric or string",
							   name);
				goto out_err;
			}
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

	ts_cache_release(&hcache);

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

	ts_cache_release(&hcache);

	if (NULL == tuple)
		ereport(ERROR,
				(errcode(ERRCODE_TS_INTERNAL_ERROR), errmsg("could not create tuple from chunk")));

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

/*
 * Detach a chunk from a hypertable.
 */
Datum
chunk_detach(PG_FUNCTION_ARGS)
{
	Oid chunk_relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	Cache *hcache;
	Hypertable *ht;
	Chunk *chunk;
	Oid ht_rel;

	TS_PREVENT_FUNC_IF_READ_ONLY();

	if (!OidIsValid(chunk_relid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid chunk relation OID")));

	DEBUG_WAITPOINT("chunk_detach_before_lock");

	ht_rel = ts_hypertable_id_to_relid(ts_chunk_get_hypertable_id_by_reloid(chunk_relid), true);
	if (!OidIsValid(ht_rel))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("hypertable not found for the chunk")));

	/* Take the same locks taken by PostgreSQL partitioning to be consistent */
	LockRelationOid(ht_rel, ShareUpdateExclusiveLock);
	LockRelationOid(chunk_relid, AccessExclusiveLock);

	chunk = ts_chunk_get_by_relid(chunk_relid, true);
	Assert(chunk != NULL);

	ht = ts_hypertable_cache_get_cache_and_entry(chunk->hypertable_relid, CACHE_FLAG_NONE, &hcache);
	Assert(ht != NULL);

	if (!object_ownercheck(RelationRelationId, ht->main_table_relid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER,
					   get_relkind_objtype(get_rel_relkind(ht->main_table_relid)),
					   get_rel_name(ht->main_table_relid));

	if (ts_chunk_is_compressed(chunk))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot detach compressed chunk \"%s\"", get_rel_name(chunk_relid)),
				 errhint("Decompress the chunk first.")));

	if (chunk->fd.osm_chunk)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot detach OSM chunk \"%s\"", get_rel_name(chunk_relid))));

	AlterTableCmd cmd = {
		.type = T_AlterTableCmd,
		.subtype = AT_DropInherit,
		.def = (Node *) makeRangeVar(NameStr(ht->fd.schema_name), NameStr(ht->fd.table_name), 0),
	};
	AlterTableStmt stmt = {
		.type = T_AlterTableStmt,
		.cmds = list_make1(&cmd),
		.relation = makeRangeVar(NameStr(ht->fd.schema_name), NameStr(ht->fd.table_name), 0),
	};

	ts_alter_table_with_event_trigger(chunk->table_id, (Node *) &stmt, list_make1(&cmd), false);

	ts_chunk_detach_by_relid(chunk_relid);

	ts_cache_release(&hcache);

	PG_RETURN_VOID();
}

/*
 * Attach an existing relation to a hypertable as a chunk.
 */
Datum
chunk_attach(PG_FUNCTION_ARGS)
{
	Oid ht_relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	Oid chunk_relid = PG_ARGISNULL(1) ? InvalidOid : PG_GETARG_OID(1);
	Jsonb *slices = PG_ARGISNULL(2) ? NULL : PG_GETARG_JSONB_P(2);
	Cache *hcache;
	Hypertable *ht;
	Hypercube *hc;
	Chunk PG_USED_FOR_ASSERTS_ONLY *chunk;
	bool created;

	TS_PREVENT_FUNC_IF_READ_ONLY();

	if (!OidIsValid(chunk_relid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid chunk relation OID")));

	if (!OidIsValid(ht_relid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid hypertable relation OID")));

	if (chunk_relid == ht_relid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("chunk relation cannot be the same as hypertable relation")));

	if (NULL == slices)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid dimension slices argument"),
				 errhint("Provide a json-formatted definition of dimensional constraints for the "
						 "chunk partition.")));

	DEBUG_WAITPOINT("chunk_attach_before_lock");

	/* Take the same locks taken by PostgreSQL partitioning to be consistent */
	LockRelationOid(ht_relid, ShareUpdateExclusiveLock);
	LockRelationOid(chunk_relid, AccessExclusiveLock);

	/* Only owner is allowed */
	if (!object_ownercheck(RelationRelationId, chunk_relid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER,
					   get_relkind_objtype(get_rel_relkind(chunk_relid)),
					   get_rel_name(chunk_relid));

	if (is_inheritance_child(chunk_relid))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot attach chunk that is already a child of another table")));

	if (ts_is_hypertable(chunk_relid))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot attach hypertable as a chunk")));

	/* Check if the table still exists after taking the lock */
	if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(chunk_relid)))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("relation with OID %u does not exist", chunk_relid)));

	check_privileges_for_creating_chunk(ht_relid);
	ht = ts_hypertable_cache_get_cache_and_entry(ht_relid, CACHE_FLAG_NONE, &hcache);
	Assert(ht != NULL);

	hc = get_hypercube_from_slices(slices, ht);
	Assert(hc != NULL);

	chunk = ts_chunk_find_or_create_without_cuts(ht,
												 hc,
												 get_namespace_name(get_rel_namespace(chunk_relid)),
												 get_rel_name(chunk_relid),
												 chunk_relid,
												 &created);
	Assert(chunk != NULL);
	ts_cache_release(&hcache);

	PG_RETURN_VOID();
}
