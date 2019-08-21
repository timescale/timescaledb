/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <fmgr.h>
#include <funcapi.h>
#include <utils/timestamp.h>
#include <access/htup_details.h>

#include "bgw_policy/job.h"
#include "bgw/job_stat.h"
#include "chunk.h"
#include "reorder.h"

#define NUM_REORDER_RET_VALS 2

TS_FUNCTION_INFO_V1(ts_test_auto_reorder);
TS_FUNCTION_INFO_V1(ts_test_auto_drop_chunks);
TS_FUNCTION_INFO_V1(ts_test_auto_compress_chunks);

static Oid chunk_oid;
static Oid index_oid;

static void
dummy_reorder_func(Oid tableOid, Oid indexOid, bool verbose, Oid wait_id,
				   Oid destination_tablespace, Oid index_tablespace)
{
	chunk_oid = tableOid;
	index_oid = indexOid;
	reorder_chunk(tableOid, indexOid, true, wait_id, InvalidOid, InvalidOid);
}

Datum
ts_test_auto_reorder(PG_FUNCTION_ARGS)
{
	TupleDesc tupdesc;
	HeapTuple tuple;
	int32 job_id = PG_GETARG_INT32(0);
	Datum values[NUM_REORDER_RET_VALS];
	bool nulls[NUM_REORDER_RET_VALS] = { false };
	BgwJob job = { .fd = { .id = job_id } };

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));
	}

	execute_reorder_policy(&job, dummy_reorder_func, false);

	values[0] = ObjectIdGetDatum(chunk_oid);
	values[1] = ObjectIdGetDatum(index_oid);

	tuple = heap_form_tuple(tupdesc, values, nulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

/* Call the real drop_chunks policy */
Datum
ts_test_auto_drop_chunks(PG_FUNCTION_ARGS)
{
	execute_drop_chunks_policy(PG_GETARG_INT32(0));

	PG_RETURN_NULL();
}

/* Call the real compress_chunks policy */
Datum
ts_test_auto_compress_chunks(PG_FUNCTION_ARGS)
{
	int32 job_id = PG_GETARG_INT32(0);
	BgwJob *job = ts_bgw_job_find(job_id, CurrentMemoryContext, true);
	/*since we didn't come through the scheduler, need to mark job
	 * as started to create a job_stat record */
	ts_bgw_job_stat_mark_start(job_id);
	return execute_compress_chunks_policy(job);
}
