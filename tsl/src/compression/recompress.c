/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <funcapi.h>
#include <string.h>
#include <utils/builtins.h>
#include <utils/rel.h>
#include <executor/spi.h>

#include "chunk.h"
#include "compat.h"
#include "compression.h"
#include "hypertable_compression.h"
#include "recompress.h"

typedef struct RCQueryState
{
	TupleDesc compress_desc;
	RecompressTuple *rctuple;
	Datum *compressed_datums;
	bool *compressed_is_nulls;
} RCQueryState;

typedef struct RecompressChunkState
{
	RCQueryState *qrystate;
	RecompressTupleGroupState *grpstate;
} RecompressChunkState;

static void rc_query_shutdown(Datum arg);

static RCQueryState *
rc_query_state_init(FunctionCallInfo fcinfo, Oid uncompressed_chunk_relid)
{
	// TupleDesc in_desc;
	Chunk *chunk = ts_chunk_get_by_relid(uncompressed_chunk_relid, true);
	Oid compress_chunk_relid = ts_chunk_get_relid(chunk->fd.compressed_chunk_id, false);
	if (compress_chunk_relid == InvalidOid)
		elog(ERROR, "no compressed chunk found for %s", get_rel_name(uncompressed_chunk_relid));
	RCQueryState *state = palloc(sizeof(RCQueryState));
	Relation chunk_rel = table_open(chunk->table_id, AccessShareLock); // TODO what lock here?
	Relation compress_rel = table_open(compress_chunk_relid, AccessShareLock);
	state->rctuple = recompress_tuple_init(chunk->fd.hypertable_id, chunk_rel, compress_rel);
	state->compress_desc = RelationGetDescr(compress_rel);
	state->compressed_datums = palloc(sizeof(Datum) * state->compress_desc->natts);
	state->compressed_is_nulls = palloc(sizeof(bool) * state->compress_desc->natts);

	table_close(compress_rel, AccessShareLock); // TODO what lock here?
	table_close(chunk_rel, AccessShareLock);	// TODO what lock here?
	return state;
}

static void
rc_query_shutdown(Datum arg)
{
	RecompressChunkState *state = (RecompressChunkState *) DatumGetPointer(arg);
	recompress_tuple_group_destroy(state->grpstate);
}

static RecompressChunkState *
rc_state_setup(FunctionCallInfo fcinfo, Oid uncompressed_chunk_relid)
{
	MemoryContext qcontext, grpcontext, oldcontext;
	RCQueryState *qrystate = (RCQueryState *) fcinfo->flinfo->fn_extra;
	RecompressChunkState *tstate;

	if (qrystate == NULL) /*first time the function is called */
	{
		/* want to keep this information for the duration of the query */
		qcontext = fcinfo->flinfo->fn_mcxt;
		oldcontext = MemoryContextSwitchTo(qcontext);
		qrystate = rc_query_state_init(fcinfo, uncompressed_chunk_relid);
		fcinfo->flinfo->fn_extra = qrystate;
		MemoryContextSwitchTo(oldcontext);
	}
	if (AggCheckCallContext(fcinfo, &grpcontext) != AGG_CONTEXT_AGGREGATE)
		elog(ERROR, "recompress_chunk_tuples called in non-aggregate context");
	/* per group initialization, switch to aggregate context */
	oldcontext = MemoryContextSwitchTo(grpcontext);
	tstate = palloc(sizeof(RecompressChunkState));
	tstate->qrystate = qrystate;
	tstate->grpstate = recompress_tuple_group_init(tstate->qrystate->rctuple);
	AggRegisterCallback(fcinfo, rc_query_shutdown, PointerGetDatum(tstate));
	MemoryContextSwitchTo(oldcontext);
	return tstate;
}

/* args are (internal, oid, record) */
Datum
tsl_recompress_chunk_sfunc(PG_FUNCTION_ARGS)
{
	RecompressChunkState *tstate;
	Oid uncompressed_chunk_relid = PG_ARGISNULL(1) ? InvalidOid : PG_GETARG_OID(1);
	HeapTupleHeader rec = PG_GETARG_HEAPTUPLEHEADER(2);
	// Oid arg1_typeid = get_fn_expr_argtype(fcinfo->flinfo, 1);
	// Oid arg2_typeid = get_fn_expr_argtype(fcinfo->flinfo, 2);
	// elog(NOTICE, "typeis is %d %d", arg1_typeid, arg2_typeid);
	// my_spi_function();
	MemoryContext grp_context, old_context;

	if (!AggCheckCallContext(fcinfo, &grp_context) || !IsA(fcinfo->context, AggState))
	{
		elog(ERROR, "recompress_chunk_sfunc called in non-aggregate context");
	}
	if (PG_ARGISNULL(2) || uncompressed_chunk_relid == InvalidOid)
		elog(ERROR, "unexpected null in tsl_recompress_chunk_sfunc");

	if (PG_ARGISNULL(0))
		tstate = rc_state_setup(fcinfo, uncompressed_chunk_relid);
	else
		tstate = (RecompressChunkState *) PG_GETARG_POINTER(0);
	/* construct a tuple from passed in record */
	old_context = MemoryContextSwitchTo(grp_context);
	HeapTupleData tuple;
	tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
	ItemPointerSetInvalid(&(tuple.t_self));
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = rec;
	heap_deform_tuple(&tuple,
					  tstate->qrystate->compress_desc,
					  tstate->qrystate->compressed_datums,
					  tstate->qrystate->compressed_is_nulls);

	recompress_tuple_append_row(tstate->qrystate->rctuple,
								tstate->grpstate,
								tstate->qrystate->compressed_datums,
								tstate->qrystate->compressed_is_nulls);
	MemoryContextSwitchTo(old_context);
	PG_RETURN_POINTER(tstate);
}

/* tsl_recompress_chunk_ffunc:
 * apply the finalize function on the state we have accumulated
 */
Datum
tsl_recompress_chunk_ffunc(PG_FUNCTION_ARGS)
{
	bool grp_done = false;
	int rowcnt = 0;
	HeapTuple compressed_tuple = NULL;
	HeapTupleHeader result;
	ArrayBuildState *arrstate = NULL;
	RecompressChunkState *tstate =
		PG_ARGISNULL(0) ? NULL : (RecompressChunkState *) PG_GETARG_POINTER(0);
	Oid arg2_typeid = get_fn_expr_argtype(fcinfo->flinfo, 2);
	MemoryContext grp_context, old_context;
	Assert(tstate != NULL);
	if (!AggCheckCallContext(fcinfo, &grp_context))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "recompress_chunk_ffunc called in non-aggregate context");
	}
	old_context = MemoryContextSwitchTo(grp_context);
	// test what happens on empty table
	while ((compressed_tuple =
				recompress_tuple_get_next(tstate->qrystate->rctuple, tstate->grpstate, &grp_done)))
	{
		result = (HeapTupleHeader) palloc(compressed_tuple->t_len);
		memcpy(result, compressed_tuple->t_data, compressed_tuple->t_len);
		Datum datum = HeapTupleHeaderGetDatum(result);
		arrstate = accumArrayResult(arrstate, datum, false, arg2_typeid, CurrentMemoryContext);
		if (grp_done)
			break;
		rowcnt++;
		// break;   /* we combine vereything into 1 row */
	}
	MemoryContextSwitchTo(old_context);
	if (arrstate)
		PG_RETURN_DATUM(makeArrayResult(arrstate, CurrentMemoryContext));
	// if ( compressed_tuple )
	// PG_RETURN_HEAPTUPLEHEADER(result);
	else
		PG_RETURN_NULL();
}

#define RECOMPRESS_SEG_PRINT_ALL(querybuf, htcols_list, segorder_colindex, nseg, cstr)             \
	do                                                                                             \
	{                                                                                              \
		for (int i = 1; i < nseg; i++)                                                             \
		{                                                                                          \
			int idx = segorder_colindex[i];                                                        \
			NameData attname =                                                                     \
				((FormData_hypertable_compression *) list_nth(htcols_list, idx))->attname;         \
			appendStringInfo(querybuf, "%s %s ", cstr, NameStr(attname));                          \
		}                                                                                          \
	} while (0)

#define RECOMPRESS_SEG_GET_ATTNAME(htcols_list, segorder_colindex, segidx)                         \
	NameStr(((FormData_hypertable_compression *) list_nth(htcols_list, segorder_colindex[segidx])) \
				->attname)

static void
recompress_tuple_get_segmentby_sql(Chunk *uncompressed_chunk, StringInfoData *querybuf,
								   List *htcols_list, int16 *segorder_colindex, int nseg)
{
	Chunk *compchunk = ts_chunk_get_by_id(uncompressed_chunk->fd.compressed_chunk_id, true);
	const char *uncompchunk_name = NameStr(uncompressed_chunk->fd.table_name);
	const char *uncompchunk_schema = NameStr(uncompressed_chunk->fd.schema_name);
	const char *compchunk_name = NameStr(compchunk->fd.table_name);
	const char *compchunk_schema = NameStr(compchunk->fd.schema_name);
	const char *attname_seg0 = RECOMPRESS_SEG_GET_ATTNAME(htcols_list, segorder_colindex, 0);
	/* generate the following query string using all the segment by cols
	   WITH dsel AS (
			 SELECT distinct segcol, segcol2 FROM :COMP_CHUNK_NAME WHERE _ts_meta_sequence_num = 0)
	  , cdel AS ( DELETE FROM :COMP_CHUNK_NAME c
				 USING dsel
				 WHERE c.segcol = dsel.segcol AND c.segcol2 = dsel.segcol2 )
		INSERT INTO :COMP_CHUNK_NAME
		SELECT  (unnest(_timescaledb_internal.recompress_tuples(:'CHUNK_NAME'::regclass, c))).*
		FROM :COMP_CHUNK_NAME c , dsel
		WHERE c.segcol = dsel.segcol AND c.segcol2 = dsel.segcol2
		GROUP BY c.segcol, c.segcol2;

	*/
	appendStringInfo(querybuf, " WITH dsel AS (SELECT distinct %s ", attname_seg0);
	RECOMPRESS_SEG_PRINT_ALL(querybuf, htcols_list, segorder_colindex, nseg, ",");
	appendStringInfo(querybuf,
					 " FROM %s.%s WHERE _ts_meta_sequence_num = 0 )",
					 quote_identifier(compchunk_schema),
					 quote_identifier(compchunk_name));

	appendStringInfo(querybuf,
					 ", cdel AS ( DELETE FROM %s.%s c USING dsel WHERE ",
					 quote_identifier(compchunk_schema),
					 quote_identifier(compchunk_name));
	appendStringInfo(querybuf, "c.%s = dsel.%s ", attname_seg0, attname_seg0);
	for (int i = 1; i < nseg; i++)
	{
		// const char *attname = RECOMPRESS_SEG_GET_ATTNAME(htcols_list, segorder_colindex, i );
		appendStringInfo(querybuf,
						 "AND c.%s = dsel.%s ",
						 RECOMPRESS_SEG_GET_ATTNAME(htcols_list, segorder_colindex, i),
						 RECOMPRESS_SEG_GET_ATTNAME(htcols_list, segorder_colindex, i));
	}
	appendStringInfo(querybuf,
					 ") INSERT INTO %s.%s "
					 " SELECT  (unnest(_timescaledb_internal.recompress_tuples('%s.%s'::regclass, "
					 "c))).*"
					 " FROM %s.%s c , dsel  WHERE ",
					 quote_identifier(compchunk_schema),
					 quote_identifier(compchunk_name),
					 quote_identifier(uncompchunk_schema),
					 quote_identifier(uncompchunk_name),
					 quote_identifier(compchunk_schema),
					 quote_identifier(compchunk_name));
	appendStringInfo(querybuf, "c.%s = dsel.%s ", attname_seg0, attname_seg0);
	for (int i = 1; i < nseg; i++)
	{
		appendStringInfo(querybuf,
						 "AND c.%s = dsel.%s ",
						 RECOMPRESS_SEG_GET_ATTNAME(htcols_list, segorder_colindex, i),
						 RECOMPRESS_SEG_GET_ATTNAME(htcols_list, segorder_colindex, i));
	}
	appendStringInfo(querybuf, "GROUP BY c.%s", attname_seg0);
	for (int i = 1; i < nseg; i++)
	{
		appendStringInfo(querybuf,
						 ", c.%s ",
						 RECOMPRESS_SEG_GET_ATTNAME(htcols_list, segorder_colindex, i));
	}
}

// support only for sgement by right now
void
recompress_chunk_tuple(Chunk *uncompressed_chunk)
{
	int nseg = 0, nord = 0, i, res;
	int16 *segorder_colindex = NULL;
	ListCell *lc;
	int32 srcht_id = ts_hypertable_relid_to_id(uncompressed_chunk->hypertable_relid);
	List *htcols_list = ts_hypertable_compression_get(srcht_id);
	foreach (lc, htcols_list)
	{
		FormData_hypertable_compression *fd = lfirst(lc);
		if (fd->segmentby_column_index > 0)
			nseg++;
		if (fd->orderby_column_index > 0)
			nord++;
	}
	if (nseg > 0)
	{
		segorder_colindex = (int16 *) palloc0(sizeof(int16) * nseg);
	}
	i = 0;
	/* map the segment by column info */
	foreach (lc, htcols_list)
	{
		FormData_hypertable_compression *fd = lfirst(lc);
		/* segmentby_column_index starts at 1 */
		if (fd->segmentby_column_index > 0)
			segorder_colindex[fd->segmentby_column_index - 1] = i;
		i++;
	}
	Assert(nseg > 0);
	StringInfoData querybuf;
	initStringInfo(&querybuf);
	if (nseg > 0)
		recompress_tuple_get_segmentby_sql(uncompressed_chunk,
										   &querybuf,
										   htcols_list,
										   segorder_colindex,
										   nseg);
	elog(DEBUG, "recompress_chunk_tuple string is %s", querybuf.data);
	if (SPI_connect_ext(SPI_OPT_NONATOMIC) != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");
	if (SPI_exec(querybuf.data, 0) != SPI_OK_INSERT)
		elog(ERROR, "SPI_exec insert failed: %s", querybuf.data);
	res = SPI_finish();
	Assert(res == SPI_OK_FINISH);
	return;
}
