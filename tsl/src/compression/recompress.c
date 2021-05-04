/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <funcapi.h>
#include <string.h>
#include <utils/rel.h>

#include "chunk.h"
#include "compat.h"
#include "compression.h"
#include "recompress.h"
#include "debug_tup.h" //debugging

typedef struct RecompressChunkState
{
	TupleDesc chunk_desc;
	TupleDesc compress_desc;
	RecompressTuple *rcstate;
	Datum *compressed_datums;
	bool *compressed_is_nulls;
} RecompressChunkState;

static RecompressChunkState *
rc_query_state_init(FunctionCallInfo fcinfo, Oid uncompressed_chunk_relid)
{
	// TupleDesc in_desc;
	MemoryContext qcontext = fcinfo->flinfo->fn_mcxt;
	MemoryContext oldcontext = MemoryContextSwitchTo(qcontext);
	Chunk *chunk = ts_chunk_get_by_relid(uncompressed_chunk_relid, true);
	Oid compress_chunk_relid = ts_chunk_get_relid(chunk->fd.compressed_chunk_id, false);
	if (compress_chunk_relid == InvalidOid)
		elog(ERROR, "no compressed chunk found for %s", get_rel_name(uncompressed_chunk_relid));
	RecompressChunkState *state =
		(RecompressChunkState *) MemoryContextAlloc(qcontext, sizeof(RecompressChunkState));
	Relation chunk_rel = table_open(chunk->table_id, RowExclusiveLock); // TODO what lock here?
	Relation compress_rel = table_open(compress_chunk_relid, RowExclusiveLock);
	state->rcstate = recompress_tuple_init(chunk->fd.hypertable_id, chunk_rel, compress_rel);
	state->chunk_desc = RelationGetDescr(chunk_rel);
	state->compress_desc = RelationGetDescr(compress_rel);
	state->compressed_datums = palloc(sizeof(Datum) * state->compress_desc->natts);
	state->compressed_is_nulls = palloc(sizeof(bool) * state->compress_desc->natts);

	table_close(compress_rel, RowExclusiveLock); // TODO what lock here?
	table_close(chunk_rel, RowExclusiveLock);	// TODO what lock here?
	MemoryContextSwitchTo(oldcontext);
	return state;
}

/* args are (internal, oid, record) */
Datum
tsl_recompress_chunk_sfunc(PG_FUNCTION_ARGS)
{
	RecompressChunkState *tstate =
		PG_ARGISNULL(0) ? NULL : (RecompressChunkState *) PG_GETARG_POINTER(0);
	Oid uncompressed_chunk_relid = PG_ARGISNULL(1) ? InvalidOid : PG_GETARG_OID(1);
	HeapTupleHeader rec = PG_GETARG_HEAPTUPLEHEADER(2);

	//Oid arg1_typeid = get_fn_expr_argtype(fcinfo->flinfo, 1);
	//Oid arg2_typeid = get_fn_expr_argtype(fcinfo->flinfo, 2);
	//elog(NOTICE, "typeis is %d %d", arg1_typeid, arg2_typeid);
	MemoryContext fa_context, old_context;

	if (!AggCheckCallContext(fcinfo, &fa_context) || !IsA(fcinfo->context, AggState))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "recompress_chunk_sfunc called in non-aggregate context");
	}
	if (PG_ARGISNULL(1))
		elog(ERROR, "unexpected null record in tsl_recompress_chunk_sfunc");
	old_context = MemoryContextSwitchTo(fa_context);

	if (tstate == NULL)
	{
		tstate = (RecompressChunkState *) fcinfo->flinfo->fn_extra;
		if (fcinfo->flinfo->fn_extra == NULL)
		{
			tstate = rc_query_state_init(fcinfo, uncompressed_chunk_relid);
			fcinfo->flinfo->fn_extra = tstate;
		}
		else
		{
			/* now dealing with a new group */
			recompress_tuple_reset(tstate->rcstate);
		}
	}
	/* construct a tuple from passed in record */
	HeapTupleData tuple;
	tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
	ItemPointerSetInvalid(&(tuple.t_self));
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = rec;
	heap_deform_tuple(&tuple,
					  tstate->compress_desc,
					  tstate->compressed_datums,
					  tstate->compressed_is_nulls);

	recompress_tuple_append_row(tstate->rcstate,
								tstate->compressed_datums,
								tstate->compressed_is_nulls);
	MemoryContextSwitchTo(old_context);

	PG_RETURN_POINTER(tstate);
}

/* tsl_recompress_chunk_ffunc:
 * apply the finalize function on the state we have accumulated
 */
Datum
tsl_recompress_chunk_ffunc(PG_FUNCTION_ARGS)
{
int rowcnt = 0;
	HeapTuple compressed_tuple;
    ArrayBuildState *arrstate = NULL;
	RecompressChunkState *tstate =
		PG_ARGISNULL(0) ? NULL : (RecompressChunkState *) PG_GETARG_POINTER(0);
	Oid arg2_typeid = get_fn_expr_argtype(fcinfo->flinfo, 2);
	MemoryContext fa_context, old_context;
	Assert(tstate != NULL);
	if (!AggCheckCallContext(fcinfo, &fa_context))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "recompress_chunk_ffunc called in non-aggregate context");
	}
	old_context = MemoryContextSwitchTo(fa_context);
	// test what happens on empty table
	while ((compressed_tuple = recompress_tuple_get_next(tstate->rcstate)))
	{
       HeapTupleHeader result;
       result = (HeapTupleHeader) palloc(compressed_tuple->t_len);
       memcpy(result, compressed_tuple->t_data, compressed_tuple->t_len);
       Datum datum = HeapTupleHeaderGetDatum(result);
       arrstate = accumArrayResult(arrstate, datum, false, arg2_typeid, CurrentMemoryContext);
		//print_tuple(compressed_tuple, tstate->compress_desc);
rowcnt++;
	}
	MemoryContextSwitchTo(old_context);
elog(NOTICE, "arg type is %d rwcnt %d !!!!!!", arg2_typeid, rowcnt);
    PG_RETURN_DATUM( makeArrayResult(arrstate, CurrentMemoryContext));
}
