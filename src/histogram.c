#include <postgres.h>
#include <access/htup_details.h>
#include <catalog/namespace.h>
#include <catalog/pg_aggregate.h>
#include <catalog/pg_proc.h>
#include <catalog/pg_type.h>
#include <nodes/makefuncs.h>
#include <utils/builtins.h>
#include <utils/array.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <netinet/in.h>
#include <lib/stringinfo.h>
#include <libpq/pqformat.h>

#include "compat.h"
#include "errors.h"


#define HIST_LEN(state) ((VARSIZE(state) - VARHDRSZ ) / sizeof(Datum))
#define VALUES_SIZE(arrsize) (VARHDRSZ + arrsize * sizeof(Datum))

/* aggregate histogram:
 *	 histogram(state, val, min, max, nbuckets, bucket_agg) returns the histogram array with nbuckets
 *
 * Usage:
 *	 SELECT grouping_element, histogram(field, min, max, nbuckets, [bucket_agg]) FROM table GROUP BY grouping_element.
 *
 * Description:
 * Histogram generates a histogram array based off of a specified range passed into the function.
 * Values falling outside of this range are bucketed into the 0 or nbucket+1 buckets depending on
 * if they are below or above the range, respectively. The resultant histogram therefore contains
 * nbucket+2 buckets accounting for buckets outside the range.
 */

TS_FUNCTION_INFO_V1(hist_sfunc);
TS_FUNCTION_INFO_V1(hist_combinefunc);
TS_FUNCTION_INFO_V1(hist_serializefunc);
TS_FUNCTION_INFO_V1(hist_deserializefunc);
TS_FUNCTION_INFO_V1(hist_finalfunc);

/*
 * Contains information/state on the aggregator for each bucket, including
 * functions for de/serialization, starting the aggregate, and finalizing.
 */
typedef struct SubAggState
{
	Oid			sfunc;
	Oid			combinefunc;
	Oid			finalfunc;
	Oid			serialfunc;
	Oid			deserialfunc;
	Oid			result_type;
	bool		isstrict_sfunc;
} SubAggState;

/*
 * Contains the general Histogram state. Namely, the datums that represent each
 * bucket's values as Datums (in a bytea), whether each bucket has had its first
 * tuple (needed for strict functions), and information on how to perform each
 * aggregate.
 */
typedef struct HistogramState
{
	bytea	   *values;
	/* need to track if first non-null tuple has been found for 'strict' aggs */
	bool	   *first_tuple;
	SubAggState subagg_state;
} HistogramState;

/*
 * Contains cached information for more quickly initializing new HistogramStates
 * in, e.g., a group by. Stored in the function call's fn_extra.
 */
typedef struct HistogramInternalFnState
{
	Oid			aggfn;
	Oid			sfunc;
	Oid			combinefunc;
	Oid			finalfunc;
	Oid			serialfunc;
	Oid			deserialfunc;
	Oid			trans_type;
	Oid			result_type;
	bool		isstrict_sfunc;
	Datum		text_init_value;
	bool		init_value_isnull;
} HistogramInternalFnState;

typedef struct HistogramSerializeState
{
	Oid			fn;
	Oid			typioparam;
} HistogramSerializeState;

static inline void
set_first_tuples(HistogramState *state, bool val)
{
	Size		i;
	Size		arrsize = HIST_LEN(state->values);

	for (i = 0; i < arrsize; i++)
		state->first_tuple[i] = val;
}

/* Copied from postgres source */
static Datum
get_agg_init_val(Datum textInitVal, Oid transtype, MemoryContext ctxt)
{
	Oid			typinput;
	Oid			typioparam;
	char	   *strInitVal;
	Datum		initVal;
	MemoryContext old = MemoryContextSwitchTo(ctxt);

	getTypeInputInfo(transtype, &typinput, &typioparam);
	strInitVal = TextDatumGetCString(textInitVal);
	initVal = OidInputFunctionCall(typinput, strInitVal,
								   typioparam, -1);
	pfree(strInitVal);
	MemoryContextSwitchTo(old);
	return initVal;
}

static inline HistogramState *
init_state(MemoryContext aggcontext, Size arrsize)
{
	HistogramState *state = (HistogramState *) MemoryContextAlloc(aggcontext, sizeof(HistogramState));

	state->values = MemoryContextAllocZero(aggcontext, VALUES_SIZE(arrsize));
	state->first_tuple = MemoryContextAlloc(aggcontext, arrsize);
	SET_VARSIZE(state->values, VALUES_SIZE(arrsize));
	set_first_tuples(state, true);
	state->subagg_state = (SubAggState)
	{
		.sfunc = InvalidOid,
			.combinefunc = InvalidOid,
			.finalfunc = InvalidOid,
			.serialfunc = InvalidOid,
			.deserialfunc = InvalidOid,
			.result_type = InvalidOid,
			.isstrict_sfunc = false,
	};

	return state;
}

static Oid
find_available_aggregate(char *subagg_text)
{
	List	   *agg_fn_name = list_make1(makeString(subagg_text));
	FuncCandidateList funclist = FuncnameGetCandidates(agg_fn_name, 1, NULL,
													   false, true, false);

	/*
	 * Currently we only support histogram on FLOAT8 subtype columns, so check
	 * if we can find a FLOAT8 or ANY version of the aggregator
	 */
	while (funclist != NULL && (funclist->nargs != 1 ||
								(funclist->args[0] != FLOAT8OID && funclist->args[0] != ANYOID)))
	{
		funclist = funclist->next;
	}

	if (NULL == funclist)
		elog(ERROR, "aggregation function '%s' not found", subagg_text);

	return funclist->oid;
}

static void
set_bucket_initial_values(HistogramInternalFnState *istate, HistogramState *state,
						  MemoryContext ctxt)
{
	if (istate->init_value_isnull)
		set_first_tuples(state, true);
	else
	{
		Size		i;
		Size		arrsize = HIST_LEN(state->values);
		Datum	   *hist = (Datum *) VARDATA(state->values);

		for (i = 0; i < arrsize; i++)
			hist[i] = get_agg_init_val(istate->text_init_value,
									   istate->trans_type, ctxt);
		set_first_tuples(state, false);
	}
}

static Datum
apply_subagg_sfunc(Oid funcid, fmNodePtr ctxt, Datum arg1, Datum arg2)
{
	FmgrInfo	flinfo;
	FunctionCallInfoData fcinfo;
	Datum		res;

	fmgr_info(funcid, &flinfo);
	InitFunctionCallInfoData(fcinfo, &flinfo, 2, InvalidOid, ctxt, NULL);
	fcinfo.arg[0] = arg1;
	fcinfo.arg[1] = arg2;
	fcinfo.argnull[0] = false;
	fcinfo.argnull[1] = false;

	res = FunctionCallInvoke(&fcinfo);
	if (fcinfo.isnull)
		elog(ERROR, "function %u returned NULL", funcid);

	return res;
}

static Datum
apply_subagg_combine(Oid funcid, fmNodePtr ctxt, Datum arg1, Datum arg2)
{
	FmgrInfo	flinfo;
	FunctionCallInfoData fcinfo;
	Datum		res;

	fmgr_info(funcid, &flinfo);
	InitFunctionCallInfoData(fcinfo, &flinfo, 2, InvalidOid, ctxt, NULL);
	fcinfo.arg[0] = arg1;
	fcinfo.arg[1] = arg2;
	fcinfo.argnull[0] = false;
	fcinfo.argnull[1] = false;

	res = FunctionCallInvoke(&fcinfo);
	if (fcinfo.isnull)
		elog(ERROR, "function %u returned NULL", funcid);

	return res;
}

static Datum
apply_subagg_final(Oid funcid, fmNodePtr ctxt, Datum arg1, Datum *null_res)
{
	FmgrInfo	flinfo;
	FunctionCallInfoData fcinfo;
	Datum		res;

	fmgr_info(funcid, &flinfo);
	InitFunctionCallInfoData(fcinfo, &flinfo, 2, InvalidOid, ctxt, NULL);
	fcinfo.arg[0] = arg1;
	fcinfo.argnull[0] = false;

	res = FunctionCallInvoke(&fcinfo);
	if (fcinfo.isnull)
	{
		if (null_res == NULL)
			elog(ERROR, "function %u returned NULL", funcid);
		return *null_res;
	}

	return res;
}


/*
 * histogram(state, val, min, max, nbuckets, bucket_agg)
 *
 * state - INTERNAL state for histogram
 * val - value of the 'key' column in the tuple (numeric)
 * min - min value of histogram range (numeric)
 * max - max value of histogram range (numeric)
 * nbuckets - number of buckets to subdivide histogram range into (numeric)
 * bucket_agg - aggregator to use in each bucket (text) [optional]
 *
 * bucket_agg is optional, in which case we default to 'count' aggregator.
 * bucket_agg should be able to be found in the catalog, where the aggregator
 * takes a numeric (or supertype of numeric) as an argument.
 *
 */
Datum
hist_sfunc(PG_FUNCTION_ARGS)
{
	MemoryContext aggcontext;
	HistogramState *state = (PG_ARGISNULL(0) ? NULL : (HistogramState *) PG_GETARG_POINTER(0));
	Datum	   *hist;
	Datum		val_datum = PG_GETARG_DATUM(1);
	Datum		min_datum = PG_GETARG_DATUM(2);
	Datum		max_datum = PG_GETARG_DATUM(3);
	Datum		nbuckets_datum = PG_GETARG_DATUM(4);
	double		min = DatumGetFloat8(min_datum);
	double		max = DatumGetFloat8(max_datum);
	int			nbuckets = DatumGetInt32(nbuckets_datum);
	int32		bucket = DatumGetInt32(DirectFunctionCall4(width_bucket_float8, val_datum, min_datum, max_datum, nbuckets_datum));
	char	   *subagg_text;


	/* Cannot be called directly because of internal-type argument */
	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "hist_sfunc called in non-aggregate context");

	/* Cannot generate a histogram with incompatible bounds */
	if (min > max)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("lower bound cannot exceed upper bound")));

	/* Check if bucket_agg is set, otherwise use 'count' */
	if (PG_NARGS() == 5 || PG_ARGISNULL(5))
		subagg_text = "count";
	else
		subagg_text = TextDatumGetCString(PG_GETARG_CSTRING(5));

	if (state == NULL)
	{
		/* Length is nbuckets plus 1 on either side */
		Size		arrsize = nbuckets + 2;
		HistogramInternalFnState *istate =
		(HistogramInternalFnState *) fcinfo->flinfo->fn_extra;

		if (NULL == istate)
		{
			fcinfo->flinfo->fn_extra = MemoryContextAllocZero(fcinfo->flinfo->fn_mcxt, sizeof(HistogramInternalFnState));
			istate = (HistogramInternalFnState *) fcinfo->flinfo->fn_extra;
			istate->aggfn = InvalidOid;
		}

		state = init_state(aggcontext, arrsize);

		/* only need to set this state once so Histogram share an subagg */
		if (!OidIsValid(istate->aggfn))
		{
			HeapTuple	agg_tuple;
			HeapTuple	proc_tuple;
			Form_pg_aggregate agg_form;
			Form_pg_proc proc_form;

			istate->aggfn = find_available_aggregate(subagg_text);

			agg_tuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(istate->aggfn));
			if (!HeapTupleIsValid(agg_tuple))	/* should not happen */
				elog(ERROR, "cache lookup failed for aggregate %u", istate->aggfn);
			agg_form = (Form_pg_aggregate) GETSTRUCT(agg_tuple);

			proc_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(agg_form->aggtransfn));
			if (!HeapTupleIsValid(proc_tuple))	/* should not happen */
				elog(ERROR, "cache lookup failed for proc %u", agg_form->aggtransfn);
			proc_form = (Form_pg_proc) GETSTRUCT(proc_tuple);

			istate->trans_type = agg_form->aggtranstype;
			istate->text_init_value = SysCacheGetAttr(
													  AGGFNOID, agg_tuple, Anum_pg_aggregate_agginitval,
													  &istate->init_value_isnull);

			istate->sfunc = agg_form->aggtransfn;
			istate->combinefunc = agg_form->aggcombinefn;
			istate->finalfunc = agg_form->aggfinalfn;
			istate->serialfunc = agg_form->aggserialfn;
			istate->deserialfunc = agg_form->aggdeserialfn;

			istate->isstrict_sfunc = proc_form->proisstrict;
			istate->result_type = proc_form->prorettype;
			ReleaseSysCache(proc_tuple);
			ReleaseSysCache(agg_tuple);
		}

		set_bucket_initial_values(istate, state, aggcontext);
		state->subagg_state.sfunc = istate->sfunc;
		state->subagg_state.combinefunc = istate->combinefunc;
		state->subagg_state.finalfunc = istate->finalfunc;
		state->subagg_state.serialfunc = istate->serialfunc;
		state->subagg_state.deserialfunc = istate->deserialfunc;

		state->subagg_state.isstrict_sfunc = istate->isstrict_sfunc;
		state->subagg_state.result_type = istate->result_type;

	}
	/* Update histogram based on bucket_agg/subagg */
	hist = (Datum *) VARDATA(state->values);
	if (state->first_tuple[bucket] && state->subagg_state.isstrict_sfunc)
	{
		hist[bucket] = val_datum;
		state->first_tuple[bucket] = false;
	}
	else
		hist[bucket] = apply_subagg_sfunc(state->subagg_state.sfunc,
										  fcinfo->context, hist[bucket],
										  val_datum);

	PG_RETURN_BYTEA_P(state);
}

/* Make a copy of the internal state */
static inline HistogramState *
copy_state(MemoryContext aggcontext, HistogramState *state)
{
	Size		arrsize = HIST_LEN(state->values);
	HistogramState *copy = init_state(aggcontext, arrsize);

	memcpy(copy->values, state->values, VALUES_SIZE(arrsize));
	memcpy(copy->first_tuple, state->first_tuple, arrsize);
	copy->subagg_state = (SubAggState)
	{
		.sfunc = state->subagg_state.sfunc,
			.combinefunc = state->subagg_state.combinefunc,
			.finalfunc = state->subagg_state.finalfunc,
			.result_type = state->subagg_state.result_type,
			.isstrict_sfunc = state->subagg_state.isstrict_sfunc
	};

	return copy;
}

/* hist_combinefunc(internal, internal) => internal */
Datum
hist_combinefunc(PG_FUNCTION_ARGS)
{
	MemoryContext aggcontext;
	MemoryContext old;
	HistogramState *state1 = (PG_ARGISNULL(0) ? NULL : (HistogramState *) PG_GETARG_POINTER(0));
	HistogramState *state2 = (PG_ARGISNULL(1) ? NULL : (HistogramState *) PG_GETARG_POINTER(1));
	HistogramState *result;

	/* cannot be called directly because of internal-type argument */
	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "hist_combinefunc called in non-aggregate context");


	old = MemoryContextSwitchTo(aggcontext);
	if (state2 == NULL)
		result = state1;
	else if (state1 == NULL)
		result = state2;
	else
	{
		Datum	   *hist,
				   *hist_other;
		Size		i;

		if (!OidIsValid(state1->subagg_state.combinefunc))
		{
			MemoryContextSwitchTo(old);
			elog(ERROR, "bucket aggregate does not have a combine function, cannot be used for histogram");
		}

		result = copy_state(aggcontext, state1);
		hist = (Datum *) VARDATA(result->values);
		hist_other = (Datum *) VARDATA(state2->values);
		for (i = 0; i < HIST_LEN(result->values); i++)
			hist[i] = apply_subagg_combine(state1->subagg_state.combinefunc,
										   fcinfo->context,
										   hist[i],
										   hist_other[i]);
	}

	MemoryContextSwitchTo(old);
	PG_RETURN_POINTER(result);
}

/* hist_serializefunc(internal) => bytea */
Datum
hist_serializefunc(PG_FUNCTION_ARGS)
{
	HistogramState *state;
	StringInfoData buf;
	Datum	   *vals;
	int			i;
	HistogramSerializeState *sstate =
	(HistogramSerializeState *) fcinfo->flinfo->fn_extra;

	if (NULL == sstate)
	{
		fcinfo->flinfo->fn_extra = MemoryContextAllocZero(fcinfo->flinfo->fn_mcxt, sizeof(HistogramSerializeState));
		sstate = (HistogramSerializeState *) fcinfo->flinfo->fn_extra;
	}

	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "hist_serializefunc function called in non-aggregate context");

	Assert(!PG_ARGISNULL(0));
	state = (HistogramState *) PG_GETARG_POINTER(0);

	vals = (Datum *) VARDATA(state->values);
	pq_begintypsend(&buf);
	pq_sendint(&buf, HIST_LEN(state->values), 4);
	pq_sendint(&buf, state->subagg_state.result_type, sizeof(Oid));
	pq_sendint(&buf, state->subagg_state.sfunc, sizeof(Oid));
	pq_sendint(&buf, state->subagg_state.finalfunc, sizeof(Oid));
	pq_sendint(&buf, state->subagg_state.combinefunc, sizeof(Oid));
	pq_sendint(&buf, state->subagg_state.serialfunc, sizeof(Oid));
	pq_sendint(&buf, state->subagg_state.deserialfunc, sizeof(Oid));

	/* no serialfn given, get info to serialize each bucket based on type */
	if (!OidIsValid(state->subagg_state.serialfunc) && !OidIsValid(sstate->fn))
	{
		bool		is_varlen;

		getTypeBinaryOutputInfo(state->subagg_state.result_type, &sstate->fn,
								&is_varlen);
	}

	for (i = 0; i < HIST_LEN(state->values); i++)
	{
		bytea	   *outbytes;
		Size		sz;

		if (OidIsValid(state->subagg_state.serialfunc))
			outbytes = DatumGetByteaP(OidFunctionCall1(state->subagg_state.serialfunc, vals[i]));
		else
			outbytes = OidSendFunctionCall(sstate->fn, vals[i]);

		sz = VARSIZE(outbytes) - VARHDRSZ;
		pq_sendint(&buf, sz, 4);
		pq_sendbytes(&buf, VARDATA(outbytes), sz);
	}

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/* hist_deserializefunc(bytea, internal) => internal */
Datum
hist_deserializefunc(PG_FUNCTION_ARGS)
{
	MemoryContext aggcontext;
	MemoryContext old;
	HistogramState *result;
	bytea	   *state;
	StringInfoData buf;
	StringInfoData temp_buf;
	int			arrsize;
	int			i;
	Datum	   *vals;
	HistogramSerializeState *sstate =
	(HistogramSerializeState *) fcinfo->flinfo->fn_extra;

	if (NULL == sstate)
	{
		fcinfo->flinfo->fn_extra = MemoryContextAllocZero(fcinfo->flinfo->fn_mcxt, sizeof(HistogramSerializeState));
		sstate = (HistogramSerializeState *) fcinfo->flinfo->fn_extra;
	}

	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "hist_deserializefunc function called in non-aggregate context");

	Assert(!PG_ARGISNULL(0));
	state = PG_GETARG_BYTEA_P(0);

	initStringInfo(&buf);
	appendBinaryStringInfo(&buf, VARDATA(state), VARSIZE(state) - VARHDRSZ);

	arrsize = pq_getmsgint(&buf, 4);
	result = init_state(aggcontext, arrsize);
	result->subagg_state.result_type = (Oid) pq_getmsgint(&buf, sizeof(Oid));
	result->subagg_state.sfunc = (Oid) pq_getmsgint(&buf, sizeof(Oid));
	result->subagg_state.finalfunc = (Oid) pq_getmsgint(&buf, sizeof(Oid));
	result->subagg_state.combinefunc = (Oid) pq_getmsgint(&buf, sizeof(Oid));
	result->subagg_state.serialfunc = (Oid) pq_getmsgint(&buf, sizeof(Oid));
	result->subagg_state.deserialfunc = (Oid) pq_getmsgint(&buf, sizeof(Oid));

	/* no deserialfn given, get info to deserialize each bucket based on type */
	if (!OidIsValid(result->subagg_state.deserialfunc))
	{
		Oid			typinput;

		getTypeInputInfo(result->subagg_state.result_type, &typinput, &sstate->typioparam);
		getTypeBinaryInputInfo(result->subagg_state.result_type,
							   &sstate->fn,
							   &sstate->typioparam);
	}

	vals = (Datum *) VARDATA(result->values);
	old = MemoryContextSwitchTo(aggcontext);
	for (i = 0; i < arrsize; i++)
	{
		int			sz = pq_getmsgint(&buf, 4);

		if (OidIsValid(result->subagg_state.deserialfunc))
			vals[i] = OidFunctionCall1(result->subagg_state.deserialfunc,
									   PointerGetDatum(pq_getmsgbytes(&buf, sz)));
		else
		{
			temp_buf.data = &buf.data[buf.cursor];
			temp_buf.maxlen = sz + 1;
			temp_buf.len = sz;
			temp_buf.cursor = 0;
			buf.cursor += sz;
			buf.data[buf.cursor] = '\0';
			vals[i] = OidReceiveFunctionCall(sstate->fn, &temp_buf, sstate->typioparam, -1);
		}
	}
	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(result);
}

/* hist_funalfunc(internal, val NUMERIC, MIN REAL, MAX REAL, nbuckets INTEGER, bucket_agg TEXT) => FLOAT8[] */
Datum
hist_finalfunc(PG_FUNCTION_ARGS)
{
	HistogramState *state;
	Datum	   *hist;
	int			dims[1];
	int			lbs[1];
	Size		i;

	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "hist_finalfunc called in non-aggregate context");

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (HistogramState *) PG_GETARG_POINTER(0);
	hist = (Datum *) VARDATA(state->values);
	for (i = 0; i < HIST_LEN(state->values); i++)
	{
		if (OidIsValid(state->subagg_state.finalfunc))
		{
			Datum		null_res = DatumGetFloat8(0.0);

			hist[i] = apply_subagg_final(state->subagg_state.finalfunc, fcinfo->context, hist[i], &null_res);
		}
		if (state->subagg_state.result_type == INT8OID)
			hist[i] = Float8GetDatum((double) DatumGetInt64(hist[i]));
	}

	dims[0] = HIST_LEN(state->values);
	lbs[0] = 1;

	PG_RETURN_ARRAYTYPE_P(construct_md_array(hist, NULL, 1, dims, lbs, FLOAT8OID, 8, true, 'f'));
}
