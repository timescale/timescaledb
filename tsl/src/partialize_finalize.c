/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <fmgr.h>
#include <catalog/pg_aggregate.h>
#include <catalog/pg_type.h>
#include <utils/syscache.h>
#include <utils/datum.h>
#include <utils/builtins.h>
#include <access/htup_details.h>
#include <catalog/namespace.h>
#include <catalog/pg_collation.h>
#include <parser/parse_agg.h>

#include "compat.h"
#include "partialize_finalize.h"

/*
 * This file implements functions to split the calculation of partial and final
 * aggregates into separate steps such that the partials can be passed out of a
 * sql query (in their internal state) and then passed into another sql query to
 * be finalized and the actual aggregate returned.
 *
 * For instance: `SELECT sum(a) FROM foo;` can be transformed into `SELECT
 * finalize(partial_sum_a) FROM (SELECT partialize(sum(a)) FROM foo);`
 *
 * This is especially useful in continuous aggs, where partials are stored and
 * finalized at query time to give accurate aggregates and in the distributed
 * database in which partials are calculated by individual backend nodes and
 * then passed back to the frontend node for finalization.
 *
 * The partialize function is implemented as a regular function, the function
 * call itself does very little except ensure that the type returned is what the
 * node is expecting, most of the work is done in plan_partialize.c, where calls
 * to that function are intercepted and the plan is modified to return the
 * partial state of the aggregate rather than its finalized state. It always
 * returns a BYTEA.
 *
 * The finalize function is implemented as an aggregate which takes in the
 * schema qualified name of the agg we're finalizing (we'll call this the inner
 * agg from now on), the partial/transition state of the inner agg (as a BYTEA),
 * some collation info and the return type of the original inner agg (an
 * ANYELEMENT that will just be a null/dummy element that tells the planner what
 * type we're going to return from our agg). This function then serves basically
 * as a wrapper, it takes the transition state of the inner aggregate as its
 * input, calls the combine function of the inner aggregate as its transition
 * function and the finalfunc of the inner aggregate.
 */

/*
 * We're modeling much of our design on nodeAgg.c, and the long comment there
 * describes the design decisions well, we won't repeat all of that here, but we
 * will repeat some of it. Namely  that we want to split out as much state as
 * possible that can eventually be moved to being instantiated in an higher
 * memory context than per group as it is invariant between groups and the
 * lookups/extra memory overhead per group can have a significant impact.
 * Therefore we have 3 structs, one to define all of the invariants to apply the
 * combine function of the inner agg, one to define all of the invariants needed
 * to apply the finalize function of the inner agg and a third that carries the
 * current state of the agg in the group.
 *
 * tsl_finalize_agg_sfunc is the state transition function
 * tsl_finalize_agg_ffunc is the finalize function
 */

/* State for calling the combine + deserialize functions of the inner aggregate */
typedef struct FACombineFnMeta
{
	Oid combinefnoid;
	Oid deserialfnoid;
	Oid transtype;
	Oid recv_fn;
	Oid typIOParam;
	FmgrInfo deserialfn;
	FmgrInfo internal_deserialfn;
	FmgrInfo combinefn;
	/* either deserialfn_fcinfo or internal_deserialfn_fcinfo is valid*/
	FunctionCallInfo deserialfn_fcinfo;
	FunctionCallInfo internal_deserialfn_fcinfo;
	FunctionCallInfo combfn_fcinfo;

} FACombineFnMeta;

/* State for calling the final function of the inner aggregate */
typedef struct FAFinalFnMeta
{
	Oid finalfnoid;
	FmgrInfo finalfn;
	FunctionCallInfo finalfn_fcinfo;
} FAFinalFnMeta;

/*
 * Per group state of the finalize aggregate. Note that if we have a strict combine
 * function, both arg values have to be non-null (like min/max). When we see
 * first non-null value, initialize trans_value and set trans_value_initialized true. see PG11
 * advance_transition_function
 */
typedef struct FAPerGroupState
{
	Datum trans_value;
	bool trans_value_isnull;
	bool trans_value_initialized;
} FAPerGroupState;

/* metadata information that is common for the entire query */
typedef struct FAPerQueryState
{
	FACombineFnMeta combine_meta;
	FAFinalFnMeta final_meta;
} FAPerQueryState;

typedef struct FATransitionState
{
	FAPerQueryState *per_query_state;
	FAPerGroupState *per_group_state;
} FATransitionState;

static Oid
aggfnoid_from_aggname(text *aggfn)
{
	char *funcname = text_to_cstring(aggfn);
	Oid oid;

	oid = DatumGetObjectId(DirectFunctionCall1(regprocedurein,

											   CStringGetDatum(funcname)));

	if (!OidIsValid(oid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("function \"%s\" does not exist", funcname)));

	return oid;
}

static Oid
collation_oid_from_name(char *schema_name, char *collation_name)
{
	List *namel = NIL;
	if (NULL == collation_name)
		return InvalidOid;
	if (NULL != schema_name)
		namel = list_make1(makeString(schema_name));
	namel = lappend(namel, makeString(collation_name));
	return get_collation_oid(namel, false);
}
/*
 * deserialize from the internal format in which data is stored in bytea
 * parameter. Callers need to check deserialized_isnull . Only if this is set to false,
 * a valid value is returned.
 */
static Datum
inner_agg_deserialize(FACombineFnMeta *combine_meta, bytea *serialized_partial,
					  bool serialized_isnull, bool *deserialized_isnull)
{
	Datum deserialized = (Datum) 0;
	FunctionCallInfo deser_fcinfo = combine_meta->deserialfn_fcinfo;
	*deserialized_isnull = true;
	if (OidIsValid(combine_meta->deserialfnoid))
	{
		if (serialized_isnull && combine_meta->deserialfn.fn_strict)
		{
			PG_RETURN_VOID();
			/*don't call the deser function */
		}

		FC_ARG(deser_fcinfo, 0) = PointerGetDatum(serialized_partial);
		FC_NULL(deser_fcinfo, 0) = serialized_isnull;
		combine_meta->deserialfn_fcinfo->isnull = false;
		deserialized = FunctionCallInvoke(deser_fcinfo);
		*deserialized_isnull = deser_fcinfo->isnull;
	}
	else if (!serialized_isnull)
	{
		int32 typmod = -1;
		StringInfo string = makeStringInfo();
		FunctionCallInfo internal_deserialfn_fcinfo = combine_meta->internal_deserialfn_fcinfo;

		appendBinaryStringInfo(string,
							   VARDATA_ANY(serialized_partial),
							   VARSIZE_ANY_EXHDR(serialized_partial));
		FC_SET_ARG(internal_deserialfn_fcinfo, 0, PointerGetDatum(string));
		FC_SET_ARG(internal_deserialfn_fcinfo, 1, ObjectIdGetDatum(combine_meta->typIOParam));
		FC_SET_ARG(internal_deserialfn_fcinfo, 2, Int32GetDatum(typmod));

		internal_deserialfn_fcinfo->isnull = false;

		deserialized = FunctionCallInvoke(internal_deserialfn_fcinfo);
		*deserialized_isnull = internal_deserialfn_fcinfo->isnull;
	}
	PG_RETURN_DATUM(deserialized);
}

/* Convert a 2-dimensional array of schema, names to type OIDs */
static Oid *
get_input_types(ArrayType *input_types, size_t *number_types)
{
	ArrayMetaState meta = { .element_type = NAMEOID };
	ArrayIterator iter;
	Datum slice_datum;
	bool slice_null;
	Oid *type_oids;
	int type_index = 0;

	if (input_types == NULL)
		elog(ERROR, "cannot pass null input_type with FINALFUNC_EXTRA aggregates");

	get_typlenbyvalalign(meta.element_type, &meta.typlen, &meta.typbyval, &meta.typalign);

	if (ARR_NDIM(input_types) != 2)
		elog(ERROR, "invalid input type array: wrong number of dimensions");

	*number_types = ARR_DIMS(input_types)[0];
	type_oids = palloc0(sizeof(*type_oids) * (*number_types));

	iter = array_create_iterator(input_types, 1, &meta);

	while (array_iterate(iter, &slice_datum, &slice_null))
	{
		Datum *slice_fields;
		int slice_elems;
		Name schema;
		Name type_name;
		Oid schema_oid;
		Oid type_oid;
		ArrayType *slice_array = DatumGetArrayTypeP(slice_datum);
		if (slice_null)
			elog(ERROR, "invalid input type array slice: cannot be null");
		deconstruct_array(slice_array,
						  meta.element_type,
						  meta.typlen,
						  meta.typbyval,
						  meta.typalign,
						  &slice_fields,
						  NULL,
						  &slice_elems);
		if (slice_elems != 2)
			elog(ERROR, "invalid input type array: expecting slices of size 2");

		schema = DatumGetName(slice_fields[0]);
		type_name = DatumGetName(slice_fields[1]);

		schema_oid = get_namespace_oid(NameStr(*schema), false);
		type_oid = GetSysCacheOid2Compat(TYPENAMENSP,
										 Anum_pg_type_oid,
										 PointerGetDatum(NameStr(*type_name)),
										 ObjectIdGetDatum(schema_oid));
		if (!OidIsValid(type_oid))
			elog(ERROR, "invalid input type: %s.%s", NameStr(*schema), NameStr(*type_name));

		type_oids[type_index++] = type_oid;
	}
	return type_oids;
};

static FATransitionState *
fa_transition_state_init(MemoryContext *fa_context, FAPerQueryState *qstate, AggState *fa_aggstate)
{
	FATransitionState *tstate = NULL;
	tstate = (FATransitionState *) MemoryContextAlloc(*fa_context, sizeof(*tstate));
	tstate->per_query_state = qstate;
	tstate->per_group_state =
		(FAPerGroupState *) MemoryContextAlloc(*fa_context, sizeof(*tstate->per_group_state));

	/* Need to init tstate->per_group_state->trans_value */
	tstate->per_group_state->trans_value_isnull = true;
	tstate->per_group_state->trans_value_initialized = false;
	return tstate;
}

static FAPerQueryState *
fa_perquery_state_init(FunctionCallInfo fcinfo)
{
	char *inner_agg_input_coll_schema = PG_ARGISNULL(2) ? NULL : NameStr(*PG_GETARG_NAME(2));
	char *inner_agg_input_coll_name = PG_ARGISNULL(3) ? NULL : NameStr(*PG_GETARG_NAME(3));
	ArrayType *input_types = PG_ARGISNULL(4) ? NULL : PG_GETARG_ARRAYTYPE_P(4);
	Oid inner_agg_fn_oid = aggfnoid_from_aggname(PG_GETARG_TEXT_PP(1));

	Oid collation = collation_oid_from_name(inner_agg_input_coll_schema, inner_agg_input_coll_name);
	FAPerQueryState *tstate;
	HeapTuple inner_agg_tuple;
	Form_pg_aggregate inner_agg_form;
	MemoryContext qcontext = fcinfo->flinfo->fn_mcxt;
	MemoryContext oldcontext = MemoryContextSwitchTo(qcontext);
	AggState *fa_aggstate = (AggState *) fcinfo->context;

	/* look up catalog entry and populate what we need */
	inner_agg_tuple = SearchSysCache1(AGGFNOID, inner_agg_fn_oid);
	if (!HeapTupleIsValid(inner_agg_tuple))
		elog(ERROR, "cache lookup failed for aggregate %u", inner_agg_fn_oid);
	inner_agg_form = (Form_pg_aggregate) GETSTRUCT(inner_agg_tuple);
	/* we only support aggregates with 0 direct args (only ordered set aggs do not meet this
	 * condition)*/
	if (inner_agg_form->aggnumdirectargs != 0)
		elog(ERROR,
			 "function calls with direct args are not supported by TimescaleDB finalize agg");
	tstate = (FAPerQueryState *) MemoryContextAlloc(qcontext, sizeof(FAPerQueryState));

	tstate->final_meta.finalfnoid = inner_agg_form->aggfinalfn;
	tstate->combine_meta.combinefnoid = inner_agg_form->aggcombinefn;
	tstate->combine_meta.deserialfnoid = inner_agg_form->aggdeserialfn;
	tstate->combine_meta.transtype = inner_agg_form->aggtranstype;
	ReleaseSysCache(inner_agg_tuple);

	/* initialize combine specific state, both the deserialize function and combine function */
	if (!OidIsValid(tstate->combine_meta.combinefnoid))
		elog(ERROR,
			 "no valid combine function for the aggregate specified in Timescale finalize call");

	fmgr_info_cxt(tstate->combine_meta.combinefnoid, &tstate->combine_meta.combinefn, qcontext);
	tstate->combine_meta.combfn_fcinfo = HEAP_FCINFO(2);
	InitFunctionCallInfoData(*tstate->combine_meta.combfn_fcinfo,
							 &tstate->combine_meta.combinefn,
							 2, /* combine fn always has two args */
							 collation,
							 (void *) fa_aggstate,
							 NULL);

	if (OidIsValid(tstate->combine_meta.deserialfnoid)) /* deserial fn not necessary, no need to
															throw errors if not found */
	{
		fmgr_info_cxt(tstate->combine_meta.deserialfnoid,
					  &tstate->combine_meta.deserialfn,
					  qcontext);
		tstate->combine_meta.deserialfn_fcinfo = HEAP_FCINFO(1);
		InitFunctionCallInfoData(*tstate->combine_meta.deserialfn_fcinfo,
								 &tstate->combine_meta.deserialfn,
								 1, /* deserialize always has 1 arg */
								 collation,
								 (void *) fa_aggstate,
								 NULL);
	}
	else
	{
		/* save information for internal deserialization. caching instead
		   of calling ReceiveFunctionCall */
		getTypeBinaryInputInfo(tstate->combine_meta.transtype,
							   &tstate->combine_meta.recv_fn,
							   &tstate->combine_meta.typIOParam);
		fmgr_info_cxt(tstate->combine_meta.recv_fn,
					  &tstate->combine_meta.internal_deserialfn,
					  qcontext);
		tstate->combine_meta.internal_deserialfn_fcinfo = HEAP_FCINFO(3);
		InitFunctionCallInfoData(*tstate->combine_meta.internal_deserialfn_fcinfo,
								 &tstate->combine_meta.internal_deserialfn,
								 3,
								 InvalidOid,
								 NULL,
								 NULL);
	}
	/* initialize finalfn specific state */
	if (OidIsValid(tstate->final_meta.finalfnoid))
	{
		int num_args = 1;
		Oid *types = NULL;
		size_t number_types = 0;
		if (inner_agg_form->aggfinalextra)
		{
			types = get_input_types(input_types, &number_types);
			num_args += number_types;
		}
		if (num_args != get_func_nargs(tstate->final_meta.finalfnoid))
			elog(ERROR, "invalid number of input types");

		fmgr_info_cxt(tstate->final_meta.finalfnoid, &tstate->final_meta.finalfn, qcontext);
		/* pass the aggstate information from our current call context */
		tstate->final_meta.finalfn_fcinfo = HEAP_FCINFO(num_args);
		InitFunctionCallInfoData(*tstate->final_meta.finalfn_fcinfo,
								 &tstate->final_meta.finalfn,
								 num_args,
								 collation,
								 (void *) fa_aggstate,
								 NULL);
		if (number_types > 0)
		{
			Expr *expr;
			int i;
			build_aggregate_finalfn_expr(types,
										 num_args,
										 inner_agg_form->aggtranstype,
										 types[number_types - 1],
										 collation,
										 tstate->final_meta.finalfnoid,
										 &expr);
			fmgr_info_set_expr((Node *) expr, &tstate->final_meta.finalfn);
			for (i = 1; i < num_args; i++)
				FC_SET_NULL(tstate->final_meta.finalfn_fcinfo, i);
		}
	}
	fcinfo->flinfo->fn_extra = (void *) tstate;

	MemoryContextSwitchTo(oldcontext);

	return tstate;
}

/*
 * Take the previous value in the group state and call the combine function specified to combine
 * with the new value that's passed in.
 */
static void
group_state_advance(FAPerGroupState *per_group_state, FACombineFnMeta *combine_meta, Datum newval,
					bool newval_isnull)
{
	FC_ARG(combine_meta->combfn_fcinfo, 0) = per_group_state->trans_value;
	FC_NULL(combine_meta->combfn_fcinfo, 0) = per_group_state->trans_value_isnull;
	FC_ARG(combine_meta->combfn_fcinfo, 1) = newval;
	FC_NULL(combine_meta->combfn_fcinfo, 1) = newval_isnull;
	combine_meta->combfn_fcinfo->isnull = false;
	per_group_state->trans_value = FunctionCallInvoke(combine_meta->combfn_fcinfo);
	per_group_state->trans_value_isnull = combine_meta->combfn_fcinfo->isnull;
};
/*
 * The parameters for tsl_finalize_agg_sfunc (see util_aggregates.sql sql input names)
 * tstate The internal state of the aggregate
 * Text aggregatefn: text format of agg function whose state is passed,
 *     this should match the output of regprocedureout(<oid>)
 *     we use this to retrieve the inner_agg_fn_oid by calling regprocedurein.
 * Name inner_agg_collation_schema: schema name for input collation name used by the aggregate
 * Name inner_agg_collation_name: input collation name used by the aggregate when state was stored.
 * bytea inner_agg_serialized_state: the partial state of the inner aggregate, in its serialized
 * form (as stored in the materialization table in materialized aggs) ANYELEMENT
 * return_type_dummy_val: used for type inference of the return type, populated from the initial agg
 * node.
 *
 * We use the combine function of the aggregatefn to combine the states.
 * Respect the "strict" nature of the combine function when we encounter
 * nulls in the data.
 */
Datum
tsl_finalize_agg_sfunc(PG_FUNCTION_ARGS)
{
	FATransitionState *tstate = PG_ARGISNULL(0) ? NULL : (FATransitionState *) PG_GETARG_POINTER(0);
	bytea *inner_agg_serialized_state = PG_ARGISNULL(5) ? NULL : PG_GETARG_BYTEA_P(5);
	bool inner_agg_serialized_state_isnull = PG_ARGISNULL(5) ? true : false;
	Datum inner_agg_deserialized_state;
	MemoryContext fa_context, old_context;

	if (!AggCheckCallContext(fcinfo, &fa_context) || !IsA(fcinfo->context, AggState))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "finalize_agg_sfunc called in non-aggregate context");
	}
	if (PG_ARGISNULL(1))
		elog(ERROR, "finalize_agg_sfunc called with NULL aggfn");
	old_context = MemoryContextSwitchTo(fa_context);

	if (tstate == NULL)
	{
		FAPerQueryState *qstate = (FAPerQueryState *) fcinfo->flinfo->fn_extra;
		if (qstate == NULL)
		{
			qstate = fa_perquery_state_init(fcinfo);
			Assert(fcinfo->flinfo->fn_extra != NULL);
		}
		tstate = fa_transition_state_init(&fa_context, qstate, (AggState *) fcinfo->context);
		/* initial trans_value = the partial state of the inner agg from first invocation */
		tstate->per_group_state->trans_value =
			inner_agg_deserialize(&tstate->per_query_state->combine_meta,
								  inner_agg_serialized_state,
								  inner_agg_serialized_state_isnull,
								  &tstate->per_group_state->trans_value_isnull);
		tstate->per_group_state->trans_value_initialized =
			!(tstate->per_group_state->trans_value_isnull);
	}
	else
	{
		bool deser_isnull;
		bool call_combine;
		inner_agg_deserialized_state = inner_agg_deserialize(&tstate->per_query_state->combine_meta,
															 inner_agg_serialized_state,
															 inner_agg_serialized_state_isnull,
															 &deser_isnull);
		/*
		 * When we have a strict combine function, both arguments to combinefn
		 * have to be non-null. It also means that if we initialized our
		 * trans_value with a null value above, it doesn't actually count, so we
		 * need to try that again if so.
		 */
		call_combine = true;
		if (tstate->per_query_state->combine_meta.combinefn.fn_strict)
		{
			if (tstate->per_group_state->trans_value_initialized == false && deser_isnull == false)
			{
				/* first time we got non-null value, so init the trans_value with it*/
				tstate->per_group_state->trans_value = inner_agg_deserialized_state;
				tstate->per_group_state->trans_value_isnull = false;
				tstate->per_group_state->trans_value_initialized = true;
				call_combine = false;
			}
			else if (deser_isnull || tstate->per_group_state->trans_value_isnull)
				call_combine = false;
		}
		if (call_combine)
			group_state_advance(tstate->per_group_state,
								&tstate->per_query_state->combine_meta,
								inner_agg_deserialized_state,
								deser_isnull);
	}
	MemoryContextSwitchTo(old_context);

	PG_RETURN_POINTER(tstate);
}

/* tsl_finalize_agg_ffunc:
 * apply the finalize function on the state we have accumulated
 */
Datum
tsl_finalize_agg_ffunc(PG_FUNCTION_ARGS)
{
	FATransitionState *tstate = PG_ARGISNULL(0) ? NULL : (FATransitionState *) PG_GETARG_POINTER(0);
	MemoryContext fa_context, old_context;
	Assert(tstate != NULL);
	if (!AggCheckCallContext(fcinfo, &fa_context))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "finalize_agg_ffunc called in non-aggregate context");
	}
	old_context = MemoryContextSwitchTo(fa_context);
	if (OidIsValid(tstate->per_query_state->final_meta.finalfnoid))
	{
		/* don't execute if strict and the trans value is NULL or there are extra args (all extra
		 * args are always NULL) */
		if (!(tstate->per_query_state->final_meta.finalfn.fn_strict &&
			  tstate->per_group_state->trans_value_isnull) &&
			!(tstate->per_query_state->final_meta.finalfn.fn_strict &&
			  tstate->per_query_state->final_meta.finalfn_fcinfo->nargs > 1))
		{
			FunctionCallInfo finalfn_fcinfo = tstate->per_query_state->final_meta.finalfn_fcinfo;
			FC_ARG(finalfn_fcinfo, 0) = tstate->per_group_state->trans_value;
			FC_NULL(finalfn_fcinfo, 0) = tstate->per_group_state->trans_value_isnull;
			finalfn_fcinfo->isnull = false;

			tstate->per_group_state->trans_value = FunctionCallInvoke(finalfn_fcinfo);
			tstate->per_group_state->trans_value_isnull = finalfn_fcinfo->isnull;
		}
	}
	MemoryContextSwitchTo(old_context);
	if (tstate->per_group_state->trans_value_isnull)
		PG_RETURN_NULL();
	else
		PG_RETURN_DATUM(tstate->per_group_state->trans_value);
}

/*
 * the partialize_agg function mainly serves as a marker that the aggregate called
 * within should return a partial instead of a result. Most of the actual work
 * occurs in the planner, with the actual function just used to ensure the
 * return type is correct.
 */
Datum
tsl_partialize_agg(PG_FUNCTION_ARGS)
{
	Datum arg;
	Oid arg_type;
	Oid send_fn;
	bool type_is_varlena;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	arg = PG_GETARG_DATUM(0);
	arg_type = get_fn_expr_argtype(fcinfo->flinfo, 0);

	if (arg_type == BYTEAOID)
		PG_RETURN_DATUM(arg);

	getTypeBinaryOutputInfo(arg_type, &send_fn, &type_is_varlena);

	PG_RETURN_BYTEA_P(OidSendFunctionCall(send_fn, arg));
}
