/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/htup_details.h>
#include <access/stratnum.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <fmgr.h>
#include <lib/stringinfo.h>
#include <libpq/pqformat.h>
#include <parser/parse_oper.h>
#include <utils/builtins.h>
#include <utils/datum.h>
#include <utils/lsyscache.h>
#include <utils/sortsupport.h>
#include <utils/syscache.h>

#include "export.h"

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
	bool is_null;
	Datum datum;
} PolyDatum;

typedef struct TypeInfoCache
{
	Oid typoid;
	int16 typlen;
	bool typbyval;
} TypeInfoCache;

/* PolyDatumIOState is internal state used by  polydatum_serialize and	polydatum_deserialize  */
typedef struct PolyDatumIOState
{
	TypeInfoCache type;

	FmgrInfo proc;
	Oid typeioparam;
} PolyDatumIOState;

static PolyDatum
polydatum_from_arg(int argno, FunctionCallInfo fcinfo)
{
	PolyDatum value;

	value.is_null = PG_ARGISNULL(argno);
	if (!value.is_null)
	{
		value.datum = PG_GETARG_DATUM(argno);
	}
	else
	{
		value.datum = PointerGetDatum(NULL);
	}
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
	{
		elog(ERROR, "cache lookup failed for type %u", type_oid);
	}
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

	Assert(OidIsValid(state->type.typoid));
	polydatum_serialize_type(buf, state->type.typoid);

	if (pd->is_null)
	{
		/* emit -1 data length to signify a NULL */
		pq_sendint32(buf, -1);
		return;
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
	Oid type_oid = GetSysCacheOid2(TYPENAMENSP,
								   Anum_pg_type_oid,
								   PointerGetDatum(type_name),
								   ObjectIdGetDatum(schema_oid));
	if (!OidIsValid(type_oid))
	{
		elog(ERROR, "cache lookup failed for type %s.%s", schema_name, type_name);
	}

	return type_oid;
}

/*
 * Deserialize the PolyDatum where the binary representation is in buf.
 * If a not-null PolyDatum is passed in, fill in it's fields, otherwise palloc.
 *
 */
static PolyDatum *
polydatum_deserialize(MemoryContext mem_ctx, PolyDatum *result, StringInfo buf,
					  PolyDatumIOState *state, FunctionCallInfo fcinfo)
{
	int itemlen;
	StringInfoData item_buf;
	StringInfo bufptr;
	char csave;

	Assert(result != NULL);

	MemoryContext old_context = MemoryContextSwitchTo(mem_ctx);

	Oid deserialized_type = polydatum_deserialize_type(buf);

	/* Following is copied/adapted from record_recv in core postgres */

	/* Get and check the item length */
	itemlen = pq_getmsgint(buf, 4);
	if (itemlen < -1 || itemlen > (buf->len - buf->cursor))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
				 errmsg("insufficient data left in message %d %d", itemlen, buf->len)));
	}

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
	if (state->type.typoid != deserialized_type)
	{
		Assert(!OidIsValid(state->type.typoid));

		Oid func;
		getTypeBinaryInputInfo(deserialized_type, &func, &state->typeioparam);
		fmgr_info_cxt(func, &state->proc, fcinfo->flinfo->fn_mcxt);
		state->type.typoid = deserialized_type;
		get_typlenbyval(state->type.typoid, &state->type.typlen, &state->type.typbyval);
	}

	result->datum = ReceiveFunctionCall(&state->proc, bufptr, state->typeioparam, -1);

	if (bufptr)
	{
		/* Trouble if it didn't eat the whole buffer */
		if (item_buf.cursor != itemlen)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
					 errmsg("improper binary format in polydata")));
		}

		buf->data[buf->cursor] = csave;
	}

	MemoryContextSwitchTo(old_context);

	return result;
}

typedef struct TransCache
{
	TypeInfoCache value_type_cache;
	TypeInfoCache cmp_type_cache;
} TransCache;

/* Internal state for bookend aggregates */
typedef struct InternalCmpAggStore
{
	TransCache aggstate_type_cache;
	PolyDatum value;
	PolyDatum cmp; /* the comparison element. e.g. time */
} InternalCmpAggStore;

inline static InternalCmpAggStore *
init_store(MemoryContext aggcontext)
{
	InternalCmpAggStore *state =
		(InternalCmpAggStore *) MemoryContextAllocZero(aggcontext, sizeof(InternalCmpAggStore));
	state->value.is_null = true;
	state->cmp.is_null = true;

	return state;
}

/* State used to cache data for serialize/deserialize operations */
typedef struct InternalCmpAggStoreIOState
{
	PolyDatumIOState value;
	PolyDatumIOState cmp; /* the comparison element. e.g. time */
} InternalCmpAggStoreIOState;

inline static void
typeinfocache_polydatumcopy(TypeInfoCache *tic, PolyDatum input, PolyDatum *output)
{
	Assert(OidIsValid(tic->typoid));

	if (!tic->typbyval && !output->is_null)
	{
		pfree(DatumGetPointer(output->datum));
	}

	*output = input;

	if (!input.is_null)
	{
		output->datum = datumCopy(input.datum, tic->typbyval, tic->typlen);
		output->is_null = false;
	}
	else
	{
		output->datum = PointerGetDatum(NULL);
		output->is_null = true;
	}
}

/*
 * Sort support for the comparison element, ordered by the type's default btree
 * sort operator. That is the same order the planner uses when it turns
 * first/last into an index scan. The ordering only depends on the type, the
 * collation and the direction, so it is cached per call site and shared by all
 * groups.
 */
inline static SortSupport
cmp_sortsupport(FunctionCallInfo fcinfo, Oid type_oid, StrategyNumber strategy)
{
	SortSupport ssup = (SortSupport) fcinfo->flinfo->fn_extra;
	bool is_lt = (strategy == BTLessStrategyNumber);
	Oid lt_opr, gt_opr;

	if (ssup != NULL)
	{
		return ssup;
	}

	if (!OidIsValid(type_oid))
	{
		elog(ERROR, "could not determine the type of the comparison_element");
	}

	get_sort_group_operators(type_oid, is_lt, false, !is_lt, &lt_opr, NULL, &gt_opr, NULL);

	ssup = MemoryContextAllocZero(fcinfo->flinfo->fn_mcxt, sizeof(SortSupportData));
	ssup->ssup_cxt = fcinfo->flinfo->fn_mcxt;
	ssup->ssup_collation = fcinfo->fncollation;
	PrepareSortSupportFromOrderingOp(is_lt ? lt_opr : gt_opr, ssup);
	fcinfo->flinfo->fn_extra = ssup;

	return ssup;
}

/*
 * bookend_sfunc - internal function called by ts_last_sfunc and ts_first_sfunc;
 */
static inline Datum
bookend_sfunc(MemoryContext aggcontext, InternalCmpAggStore *state, StrategyNumber strategy,
			  FunctionCallInfo fcinfo)
{
	PolyDatum value = polydatum_from_arg(1, fcinfo);
	PolyDatum cmp = polydatum_from_arg(2, fcinfo);

	MemoryContext old_context;

	old_context = MemoryContextSwitchTo(aggcontext);

	if (state == NULL)
	{
		state = init_store(aggcontext);
		TransCache *cache = &state->aggstate_type_cache;

		TypeInfoCache *v = &cache->value_type_cache;
		v->typoid = get_fn_expr_argtype(fcinfo->flinfo, 1);
		get_typlenbyval(v->typoid, &v->typlen, &v->typbyval);

		TypeInfoCache *c = &cache->cmp_type_cache;
		c->typoid = get_fn_expr_argtype(fcinfo->flinfo, 2);
		get_typlenbyval(c->typoid, &c->typlen, &c->typbyval);

		typeinfocache_polydatumcopy(&cache->value_type_cache, value, &state->value);
		typeinfocache_polydatumcopy(&cache->cmp_type_cache, cmp, &state->cmp);
	}
	else if (!cmp.is_null)
	{
		TransCache *cache = &state->aggstate_type_cache;
		SortSupport ssup = cmp_sortsupport(fcinfo, cache->cmp_type_cache.typoid, strategy);

		/* only do comparison if cmp is not NULL */
		if (state->cmp.is_null ||
			ApplySortComparator(cmp.datum, false, state->cmp.datum, false, ssup) < 0)
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
					InternalCmpAggStore *state2, StrategyNumber strategy, FunctionCallInfo fcinfo)
{
	MemoryContext old_context;

	if (state2 == NULL)
	{
		PG_RETURN_POINTER(state1);
	}

	/*
	 * manually copy all fields from state2 to state1, as per other combine
	 * func like int8_avg_combine
	 */
	if (state1 == NULL)
	{
		old_context = MemoryContextSwitchTo(aggcontext);

		state1 = init_store(aggcontext);
		Assert(OidIsValid(state2->aggstate_type_cache.value_type_cache.typoid));
		Assert(OidIsValid(state2->aggstate_type_cache.cmp_type_cache.typoid));
		TransCache *cache1 = &state1->aggstate_type_cache;
		TransCache *cache2 = &state2->aggstate_type_cache;
		/* Take the type information from the right-hand state. */
		*cache1 = *cache2;

		typeinfocache_polydatumcopy(&cache1->value_type_cache, state2->value, &state1->value);
		typeinfocache_polydatumcopy(&cache1->cmp_type_cache, state2->cmp, &state1->cmp);

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
		{
			PG_RETURN_POINTER(state2);
		}
		else
		{
			PG_RETURN_POINTER(state1);
		}
	}

	TransCache *cache1 = &state1->aggstate_type_cache;
	SortSupport ssup = cmp_sortsupport(fcinfo, cache1->cmp_type_cache.typoid, strategy);

	if (ApplySortComparator(state2->cmp.datum, false, state1->cmp.datum, false, ssup) < 0)
	{
		old_context = MemoryContextSwitchTo(aggcontext);
		typeinfocache_polydatumcopy(&cache1->value_type_cache, state2->value, &state1->value);
		typeinfocache_polydatumcopy(&cache1->cmp_type_cache, state2->cmp, &state1->cmp);
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
	MemoryContext aggcontext;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "first_sfun called in non-aggregate context");
	}

	return bookend_sfunc(aggcontext, store, BTLessStrategyNumber, fcinfo);
}

/* last(internal internal_state, anyelement value, "any" comparison_element) */
Datum
ts_last_sfunc(PG_FUNCTION_ARGS)
{
	InternalCmpAggStore *store =
		PG_ARGISNULL(0) ? NULL : (InternalCmpAggStore *) PG_GETARG_POINTER(0);
	MemoryContext aggcontext;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "last_sfun called in non-aggregate context");
	}

	return bookend_sfunc(aggcontext, store, BTGreaterStrategyNumber, fcinfo);
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
	return bookend_combinefunc(aggcontext, state1, state2, BTLessStrategyNumber, fcinfo);
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
	return bookend_combinefunc(aggcontext, state1, state2, BTGreaterStrategyNumber, fcinfo);
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

		Oid func;
		bool is_varlena;

		my_extra->value.type = state->aggstate_type_cache.value_type_cache;
		Assert(OidIsValid(my_extra->value.type.typoid));

		getTypeBinaryOutputInfo(my_extra->value.type.typoid, &func, &is_varlena);
		fmgr_info_cxt(func, &my_extra->value.proc, fcinfo->flinfo->fn_mcxt);

		my_extra->cmp.type = state->aggstate_type_cache.cmp_type_cache;
		Assert(OidIsValid(my_extra->cmp.type.typoid));

		getTypeBinaryOutputInfo(my_extra->cmp.type.typoid, &func, &is_varlena);
		fmgr_info_cxt(func, &my_extra->cmp.proc, fcinfo->flinfo->fn_mcxt);
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
	MemoryContext aggcontext;
	bytea *sstate;
	StringInfoData buf;
	InternalCmpAggStore *result;
	InternalCmpAggStoreIOState *my_extra;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		elog(ERROR, "aggregate function called in non-aggregate context");
	}

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

	result = MemoryContextAllocZero(aggcontext, sizeof(InternalCmpAggStore));
	polydatum_deserialize(aggcontext, &result->value, &buf, &my_extra->value, fcinfo);
	polydatum_deserialize(aggcontext, &result->cmp, &buf, &my_extra->cmp, fcinfo);

	result->aggstate_type_cache.value_type_cache = my_extra->value.type;
	result->aggstate_type_cache.cmp_type_cache = my_extra->cmp.type;

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
	{
		PG_RETURN_NULL();
	}

	PG_RETURN_DATUM(state->value.datum);
}
