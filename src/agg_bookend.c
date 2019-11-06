/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <fmgr.h>
#include <access/htup_details.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <libpq/pqformat.h>
#include <lib/stringinfo.h>
#include <nodes/value.h>
#include <utils/datum.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>

#include "compat.h"

/* bookend aggregates first and last:
 *	 first(value, cmp) returns the value for the row with the smallest cmp element.
 *	 last(value, cmp) returns the value for the row with the biggest cmp element.
 *
 * Usage:
 *	 SELECT first(metric, time), last(metric, time) FROM metric GROUP BY hostname.
 */

TS_FUNCTION_INFO_V1(ts_first_sfunc);
TS_FUNCTION_INFO_V1(ts_first_combinefunc);
TS_FUNCTION_INFO_V1(ts_last_sfunc);
TS_FUNCTION_INFO_V1(ts_last_combinefunc);
TS_FUNCTION_INFO_V1(ts_bookend_finalfunc);
TS_FUNCTION_INFO_V1(ts_bookend_serializefunc);
TS_FUNCTION_INFO_V1(ts_bookend_deserializefunc);

/* A  PolyDatum represents a polymorphic datum */
typedef struct PolyDatum
{
	Oid type_oid;
	bool is_null;
	Datum datum;
} PolyDatum;

/* PolyDatumIOState is internal state used by  polydatum_serialize and	polydatum_deserialize  */
typedef struct PolyDatumIOState
{
	Oid type_oid;
	FmgrInfo proc;
	Oid typeioparam;
} PolyDatumIOState;

static PolyDatum
polydatum_from_arg(int argno, FunctionCallInfo fcinfo)
{
	PolyDatum value;

	value.type_oid = get_fn_expr_argtype(fcinfo->flinfo, argno);
	value.is_null = PG_ARGISNULL(argno);
	if (!value.is_null)
		value.datum = PG_GETARG_DATUM(argno);
	else
		value.datum = PointerGetDatum(NULL);
	return value;
}

/* Serialize type as namespace name string + type name string.
 *  Don't simple send Oid since this state may be needed across pg_dumps.
 */
static void
polydatum_serialize_type(StringInfo buf, Oid type_oid)
{
	HeapTuple tup;
	Form_pg_type type_tuple;
	char *namespace_name;

	tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_oid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for type %u", type_oid);
	type_tuple = (Form_pg_type) GETSTRUCT(tup);
	namespace_name = get_namespace_name(type_tuple->typnamespace);

	/* send qualified type name */
	pq_sendstring(buf, namespace_name);
	pq_sendstring(buf, NameStr(type_tuple->typname));

	ReleaseSysCache(tup);
}

/* serializes the polydatum pd unto buf */
static void
polydatum_serialize(PolyDatum *pd, StringInfo buf, PolyDatumIOState *state, FunctionCallInfo fcinfo)
{
	bytea *outputbytes;

	polydatum_serialize_type(buf, pd->type_oid);

	if (pd->is_null)
	{
		/* emit -1 data length to signify a NULL */
		pq_sendint32(buf, -1);
		return;
	}

	if (state->type_oid != pd->type_oid)
	{
		Oid func;
		bool is_varlena;

		getTypeBinaryOutputInfo(pd->type_oid, &func, &is_varlena);
		fmgr_info_cxt(func, &state->proc, fcinfo->flinfo->fn_mcxt);
		state->type_oid = pd->type_oid;
	}
	outputbytes = SendFunctionCall(&state->proc, pd->datum);
	pq_sendint32(buf, VARSIZE(outputbytes) - VARHDRSZ);
	pq_sendbytes(buf, VARDATA(outputbytes), VARSIZE(outputbytes) - VARHDRSZ);
}

static Oid
polydatum_deserialize_type(StringInfo buf)
{
	const char *schema_name = pq_getmsgstring(buf);
	const char *type_name = pq_getmsgstring(buf);
	Oid schema_oid = LookupExplicitNamespace(schema_name, false);
	Oid type_oid = GetSysCacheOid2Compat(TYPENAMENSP,
										 Anum_pg_type_oid,
										 PointerGetDatum(type_name),
										 ObjectIdGetDatum(schema_oid));
	if (!OidIsValid(type_oid))
		elog(ERROR, "cache lookup failed for type %s.%s", schema_name, type_name);

	return type_oid;
}

/*
 * Deserialize the PolyDatum where the binary representation is in buf.
 * If a not-null PolyDatum is passed in, fill in it's fields, otherwise palloc.
 *
 */
static PolyDatum *
polydatum_deserialize(PolyDatum *result, StringInfo buf, PolyDatumIOState *state,
					  FunctionCallInfo fcinfo)
{
	int itemlen;
	StringInfoData item_buf;
	StringInfo bufptr;
	char csave;

	if (NULL == result)
	{
		result = palloc(sizeof(PolyDatum));
	}

	result->type_oid = polydatum_deserialize_type(buf);

	/* Following is copied/adapted from record_recv in core postgres */

	/* Get and check the item length */
	itemlen = pq_getmsgint(buf, 4);
	if (itemlen < -1 || itemlen > (buf->len - buf->cursor))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
				 errmsg("insufficient data left in message %d %d", itemlen, buf->len)));

	if (itemlen == -1)
	{
		/* -1 length means NULL */
		result->is_null = true;
		bufptr = NULL;
		csave = 0;
	}
	else
	{
		/*
		 * Rather than copying data around, we just set up a phony StringInfo
		 * pointing to the correct portion of the input buffer. We assume we
		 * can scribble on the input buffer so as to maintain the convention
		 * that StringInfos have a trailing null.
		 */
		item_buf.data = &buf->data[buf->cursor];
		item_buf.maxlen = itemlen + 1;
		item_buf.len = itemlen;
		item_buf.cursor = 0;

		buf->cursor += itemlen;

		csave = buf->data[buf->cursor];
		buf->data[buf->cursor] = '\0';

		bufptr = &item_buf;
		result->is_null = false;
	}

	/* Now call the column's receiveproc */
	if (state->type_oid != result->type_oid)
	{
		Oid func;

		getTypeBinaryInputInfo(result->type_oid, &func, &state->typeioparam);
		fmgr_info_cxt(func, &state->proc, fcinfo->flinfo->fn_mcxt);
		state->type_oid = result->type_oid;
	}

	result->datum = ReceiveFunctionCall(&state->proc, bufptr, state->typeioparam, -1);

	if (bufptr)
	{
		/* Trouble if it didn't eat the whole buffer */
		if (item_buf.cursor != itemlen)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
					 errmsg("improper binary format in polydata")));

		buf->data[buf->cursor] = csave;
	}
	return result;
}

/* Internal state for bookend aggregates */
typedef struct InternalCmpAggStore
{
	PolyDatum value;
	PolyDatum cmp; /* the comparison element. e.g. time */
} InternalCmpAggStore;

/* State used to cache data for serialize/deserialize operations */
typedef struct InternalCmpAggStoreIOState
{
	PolyDatumIOState value;
	PolyDatumIOState cmp; /* the comparison element. e.g. time */
} InternalCmpAggStoreIOState;

typedef struct TypeInfoCache
{
	Oid type_oid;
	int16 typelen;
	bool typebyval;
} TypeInfoCache;

inline static void
typeinfocache_init(TypeInfoCache *tic)
{
	tic->type_oid = InvalidOid;
}

inline static void
typeinfocache_polydatumcopy(TypeInfoCache *tic, PolyDatum input, PolyDatum *output)
{
	if (tic->type_oid != input.type_oid)
	{
		tic->type_oid = input.type_oid;
		get_typlenbyval(tic->type_oid, &tic->typelen, &tic->typebyval);
	}
	*output = input;
	if (!input.is_null)
	{
		output->datum = datumCopy(input.datum, tic->typebyval, tic->typelen);
		output->is_null = false;
	}
	else
	{
		output->datum = PointerGetDatum(NULL);
		output->is_null = true;
	}
}

typedef struct CmpFuncCache
{
	Oid cmp_type;
	char op;
	FmgrInfo proc;
} CmpFuncCache;

inline static void
cmpfunccache_init(CmpFuncCache *cache)
{
	cache->cmp_type = InvalidOid;
}

inline static bool
cmpfunccache_cmp(CmpFuncCache *cache, FunctionCallInfo fcinfo, char *opname, PolyDatum left,
				 PolyDatum right)
{
	Assert(left.type_oid == right.type_oid);
	Assert(opname[1] == '\0');

	if (cache->cmp_type != left.type_oid || cache->op != opname[0])
	{
		Oid cmp_op, cmp_regproc;

		if (!OidIsValid(left.type_oid))
			elog(ERROR, "could not determine the type of the comparison_element");
		cmp_op = OpernameGetOprid(list_make1(makeString(opname)), left.type_oid, left.type_oid);
		if (!OidIsValid(cmp_op))
			elog(ERROR, "could not find a %s operator for type %d", opname, left.type_oid);
		cmp_regproc = get_opcode(cmp_op);
		if (!OidIsValid(cmp_regproc))
			elog(ERROR,
				 "could not find the procedure for the %s operator for type %d",
				 opname,
				 left.type_oid);
		fmgr_info_cxt(cmp_regproc, &cache->proc, fcinfo->flinfo->fn_mcxt);
	}
	return DatumGetBool(
		FunctionCall2Coll(&cache->proc, fcinfo->fncollation, left.datum, right.datum));
}

typedef struct TransCache
{
	TypeInfoCache value_type_cache;
	TypeInfoCache cmp_type_cache;
	CmpFuncCache cmp_func_cache;
} TransCache;

static TransCache *
transcache_get(FunctionCallInfo fcinfo)
{
	TransCache *my_extra = (TransCache *) fcinfo->flinfo->fn_extra;

	if (my_extra == NULL)
	{
		fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt, sizeof(TransCache));
		my_extra = (TransCache *) fcinfo->flinfo->fn_extra;
		typeinfocache_init(&my_extra->value_type_cache);
		typeinfocache_init(&my_extra->cmp_type_cache);
		cmpfunccache_init(&my_extra->cmp_func_cache);
	}
	return my_extra;
}

/*
 * bookend_sfunc - internal function called by ts_last_sfunc and ts_first_sfunc;
 */
static inline Datum
bookend_sfunc(MemoryContext aggcontext, InternalCmpAggStore *state, PolyDatum value, PolyDatum cmp,
			  char *opname, FunctionCallInfo fcinfo)
{
	MemoryContext old_context;
	TransCache *cache = transcache_get(fcinfo);

	old_context = MemoryContextSwitchTo(aggcontext);

	if (state == NULL)
	{
		state = (InternalCmpAggStore *) MemoryContextAlloc(aggcontext, sizeof(InternalCmpAggStore));
		typeinfocache_polydatumcopy(&cache->value_type_cache, value, &state->value);
		typeinfocache_polydatumcopy(&cache->cmp_type_cache, cmp, &state->cmp);
	}
	else
	{
		/* only do comparison if cmp is not NULL */
		if (!cmp.is_null &&
			cmpfunccache_cmp(&cache->cmp_func_cache, fcinfo, opname, cmp, state->cmp))
		{
			typeinfocache_polydatumcopy(&cache->value_type_cache, value, &state->value);
			typeinfocache_polydatumcopy(&cache->cmp_type_cache, cmp, &state->cmp);
		}
	}
	MemoryContextSwitchTo(old_context);

	PG_RETURN_POINTER(state);
}

/* bookend_combinefunc - internal function called by ts_last_combinefunc and ts_first_combinefunc;
 * fmgr args are: (internal internal_state, internal2 internal_state)
 */
static inline Datum
bookend_combinefunc(MemoryContext aggcontext, InternalCmpAggStore *state1,
					InternalCmpAggStore *state2, char *opname, FunctionCallInfo fcinfo)
{
	MemoryContext old_context;
	TransCache *cache;

	if (state2 == NULL)
		PG_RETURN_POINTER(state1);

	cache = transcache_get(fcinfo);

	/*
	 * manually copy all fields from state2 to state1, as per other combine
	 * func like int8_avg_combine
	 */
	if (state1 == NULL)
	{
		old_context = MemoryContextSwitchTo(aggcontext);

		state1 =
			(InternalCmpAggStore *) MemoryContextAlloc(aggcontext, sizeof(InternalCmpAggStore));
		typeinfocache_polydatumcopy(&cache->value_type_cache, state2->value, &state1->value);
		typeinfocache_polydatumcopy(&cache->cmp_type_cache, state2->cmp, &state1->cmp);

		MemoryContextSwitchTo(old_context);
		PG_RETURN_POINTER(state1);
	}

	if (state1->cmp.is_null && state2->cmp.is_null)
	{
		PG_RETURN_POINTER(state1);
	}
	else if (state1->cmp.is_null != state2->cmp.is_null)
	{
		if (state1->cmp.is_null)
			PG_RETURN_POINTER(state2);
		else
			PG_RETURN_POINTER(state1);
	}
	else if (cmpfunccache_cmp(&cache->cmp_func_cache, fcinfo, opname, state2->cmp, state1->cmp))
	{
		old_context = MemoryContextSwitchTo(aggcontext);
		typeinfocache_polydatumcopy(&cache->value_type_cache, state2->value, &state1->value);
		typeinfocache_polydatumcopy(&cache->cmp_type_cache, state2->cmp, &state1->cmp);
		MemoryContextSwitchTo(old_context);
	}

	PG_RETURN_POINTER(state1);
}

/* first(internal internal_state, anyelement value, "any" comparison_element) */
Datum
ts_first_sfunc(PG_FUNCTION_ARGS)
{
	InternalCmpAggStore *store =
		PG_ARGISNULL(0) ? NULL : (InternalCmpAggStore *) PG_GETARG_POINTER(0);
	PolyDatum value = polydatum_from_arg(1, fcinfo);
	PolyDatum cmp = polydatum_from_arg(2, fcinfo);
	MemoryContext aggcontext;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "first_sfun called in non-aggregate context");
	}

	return bookend_sfunc(aggcontext, store, value, cmp, "<", fcinfo);
}

/* last(internal internal_state, anyelement value, "any" comparison_element) */
Datum
ts_last_sfunc(PG_FUNCTION_ARGS)
{
	InternalCmpAggStore *store =
		PG_ARGISNULL(0) ? NULL : (InternalCmpAggStore *) PG_GETARG_POINTER(0);
	PolyDatum value = polydatum_from_arg(1, fcinfo);
	PolyDatum cmp = polydatum_from_arg(2, fcinfo);
	MemoryContext aggcontext;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "last_sfun called in non-aggregate context");
	}

	return bookend_sfunc(aggcontext, store, value, cmp, ">", fcinfo);
}

/* first_combinerfunc(internal, internal) => internal */
Datum
ts_first_combinefunc(PG_FUNCTION_ARGS)
{
	MemoryContext aggcontext;
	InternalCmpAggStore *state1 =
		PG_ARGISNULL(0) ? NULL : (InternalCmpAggStore *) PG_GETARG_POINTER(0);
	InternalCmpAggStore *state2 =
		PG_ARGISNULL(1) ? NULL : (InternalCmpAggStore *) PG_GETARG_POINTER(1);

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "ts_first_combinefunc called in non-aggregate context");
	}
	return bookend_combinefunc(aggcontext, state1, state2, "<", fcinfo);
}

/* last_combinerfunc(internal, internal) => internal */
Datum
ts_last_combinefunc(PG_FUNCTION_ARGS)
{
	MemoryContext aggcontext;
	InternalCmpAggStore *state1 =
		PG_ARGISNULL(0) ? NULL : (InternalCmpAggStore *) PG_GETARG_POINTER(0);
	InternalCmpAggStore *state2 =
		PG_ARGISNULL(1) ? NULL : (InternalCmpAggStore *) PG_GETARG_POINTER(1);

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "ts_last_combinefunc called in non-aggregate context");
	}
	return bookend_combinefunc(aggcontext, state1, state2, ">", fcinfo);
}

/* ts_bookend_serializefunc(internal) => bytea */
Datum
ts_bookend_serializefunc(PG_FUNCTION_ARGS)
{
	StringInfoData buf;
	InternalCmpAggStoreIOState *my_extra;
	InternalCmpAggStore *state;

	Assert(!PG_ARGISNULL(0));
	state = (InternalCmpAggStore *) PG_GETARG_POINTER(0);

	my_extra = (InternalCmpAggStoreIOState *) fcinfo->flinfo->fn_extra;
	if (my_extra == NULL)
	{
		fcinfo->flinfo->fn_extra =
			MemoryContextAllocZero(fcinfo->flinfo->fn_mcxt, sizeof(InternalCmpAggStoreIOState));
		my_extra = (InternalCmpAggStoreIOState *) fcinfo->flinfo->fn_extra;
	}
	pq_begintypsend(&buf);
	polydatum_serialize(&state->value, &buf, &my_extra->value, fcinfo);
	polydatum_serialize(&state->cmp, &buf, &my_extra->cmp, fcinfo);
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/* ts_bookend_deserializefunc(bytea, internal) => internal */
Datum
ts_bookend_deserializefunc(PG_FUNCTION_ARGS)
{
	bytea *sstate;
	StringInfoData buf;
	InternalCmpAggStore *result;
	InternalCmpAggStoreIOState *my_extra;

	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "aggregate function called in non-aggregate context");

	sstate = PG_GETARG_BYTEA_P(0);

	/*
	 * Copy the bytea into a StringInfo so that we can "receive" it using the
	 * standard recv-function infrastructure.
	 */
	initStringInfo(&buf);
	appendBinaryStringInfo(&buf, VARDATA(sstate), VARSIZE(sstate) - VARHDRSZ);

	my_extra = (InternalCmpAggStoreIOState *) fcinfo->flinfo->fn_extra;
	if (my_extra == NULL)
	{
		fcinfo->flinfo->fn_extra =
			MemoryContextAllocZero(fcinfo->flinfo->fn_mcxt, sizeof(InternalCmpAggStoreIOState));
		my_extra = (InternalCmpAggStoreIOState *) fcinfo->flinfo->fn_extra;
	}

	result = palloc(sizeof(InternalCmpAggStore));
	polydatum_deserialize(&result->value, &buf, &my_extra->value, fcinfo);
	polydatum_deserialize(&result->cmp, &buf, &my_extra->cmp, fcinfo);
	PG_RETURN_POINTER(result);
}

/* ts_bookend_finalfunc(internal, anyelement, "any") => anyelement */
Datum
ts_bookend_finalfunc(PG_FUNCTION_ARGS)
{
	InternalCmpAggStore *state;

	if (!AggCheckCallContext(fcinfo, NULL))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "ts_bookend_finalfunc called in non-aggregate context");
	}

	state = PG_ARGISNULL(0) ? NULL : (InternalCmpAggStore *) PG_GETARG_POINTER(0);

	if (state == NULL || state->value.is_null || state->cmp.is_null)
		PG_RETURN_NULL();

	PG_RETURN_DATUM(state->value.datum);
}
