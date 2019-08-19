/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
/*
 * This file contains source code that was copied and/or modified from
 * the PostgreSQL database, which is licensed under the open-source
 * PostgreSQL License. Please see the NOTICE at the top level
 * directory for a copy of the PostgreSQL License.
 */
#include <postgres.h>
#include <access/htup.h>
#include <access/htup_details.h>
#include <utils/sampling.h>
#include <utils/rel.h>
#include <foreign/fdwapi.h>
#include <funcapi.h>
#include <miscadmin.h>

#include <remote/dist_txn.h>
#include <remote/async.h>
#include <remote/cursor.h>

#include "utils.h"
#include "analyze.h"
#include "deparse.h"

/*
 * Workspace for analyzing a foreign table.
 */
typedef struct TsFdwAnalyzeState
{
	Relation rel; /* relcache entry for the foreign table */
	Cursor *cursor;
	List *retrieved_attrs; /* attr numbers retrieved by query */

	/* collected sample rows */
	HeapTuple *rows; /* array of size targrows */
	int targrows;	/* target # of sample rows */
	int numrows;	 /* # of sample rows collected */

	/* for random sampling */
	double samplerows;		   /* # of rows fetched */
	double rowstoskip;		   /* # of rows to skip before next sample */
	ReservoirStateData rstate; /* state for reservoir sampling */
} TsFdwAnalyzeState;

/*
 * Collect sample rows from the result of query.
 *	 - Use all tuples in sample until target # of samples are collected.
 *	 - Subsequently, replace already-sampled tuples randomly.
 */
static void
analyze_row_processor(int row, TsFdwAnalyzeState *astate)
{
	int targrows = astate->targrows;
	int pos; /* array index to store tuple in */

	/* Always increment sample row counter. */
	astate->samplerows += 1;

	/*
	 * Determine the slot where this sample row should be stored.  Set pos to
	 * negative value to indicate the row should be skipped.
	 */
	if (astate->numrows < targrows)
	{
		/* First targrows rows are always included into the sample */
		pos = astate->numrows++;
	}
	else
	{
		/*
		 * Now we start replacing tuples in the sample until we reach the end
		 * of the relation.  Same algorithm as in acquire_sample_rows in
		 * analyze.c; see Jeff Vitter's paper.
		 */
		if (astate->rowstoskip < 0)
			astate->rowstoskip =
				reservoir_get_next_S(&astate->rstate, astate->samplerows, targrows);

		if (astate->rowstoskip <= 0)
		{
			/* Choose a random reservoir element to replace. */
			pos = (int) (targrows * sampler_random_fract(astate->rstate.randstate));
			Assert(pos >= 0 && pos < targrows);
			heap_freetuple(astate->rows[pos]);
		}
		else
		{
			/* Skip this tuple. */
			pos = -1;
		}

		astate->rowstoskip -= 1;
	}

	if (pos >= 0)
		astate->rows[pos] = remote_cursor_get_tuple(astate->cursor, row);
}

/*
 * Acquire a random sample of rows from foreign table.
 *
 * We fetch the whole table from the remote side and pick out some sample rows.
 *
 * Selected rows are returned in the caller-allocated array rows[],
 * which must have at least targrows entries.
 * The actual number of rows selected is returned as the function result.
 * We also count the total number of rows in the table and return it into
 * *totalrows.  Note that *totaldeadrows is always set to 0.
 *
 * Note that the returned list of rows is not always in order by physical
 * position in the table.  Therefore, correlation estimates derived later
 * may be meaningless, but it's OK because we don't use the estimates
 * currently (the planner only pays attention to correlation for indexscans).
 */
int
fdw_acquire_sample_rows(Relation relation, Oid serverid, int fetch_size, int elevel,
						HeapTuple *rows, int targrows, double *totalrows, double *totaldeadrows)
{
	TsFdwAnalyzeState astate;
	TSConnection *conn;
	TSConnectionId id = remote_connection_id(serverid, relation->rd_rel->relowner);
	StringInfoData sql;

	/* Initialize workspace state */
	astate.rel = relation;
	astate.cursor = NULL;
	astate.rows = rows;
	astate.targrows = targrows;
	astate.numrows = 0;
	astate.samplerows = 0;
	astate.rowstoskip = -1; /* -1 means not set yet */
	reservoir_init_selection_state(&astate.rstate, targrows);

	/*
	 * Get the connection to use.  We do the remote access as the table's
	 * owner, even if the ANALYZE was started by some other user.
	 */
	conn = remote_dist_txn_get_connection(id, REMOTE_TXN_NO_PREP_STMT);

	/*
	 * Construct cursor that retrieves whole rows from remote.
	 */

	initStringInfo(&sql);
	deparseAnalyzeSql(&sql, relation, &astate.retrieved_attrs);
	astate.cursor =
		remote_cursor_create_for_rel(conn, relation, astate.retrieved_attrs, sql.data, NULL);

	remote_cursor_set_fetch_size(astate.cursor, fetch_size);

	/* Make sure tuples are stored in the caller's memory context (anl_cxt)
	 * and not the batch context of cursor. */
	remote_cursor_set_tuple_memcontext(astate.cursor, CurrentMemoryContext);

	/* Retrieve and process rows a batch at a time. */
	for (;;)
	{
		int numrows;
		int i;

		/* Allow users to cancel long query */
		CHECK_FOR_INTERRUPTS();

		/*
		 * XXX possible future improvement: if rowstoskip is large, we
		 * could issue a MOVE rather than physically fetching the rows,
		 * then just adjust rowstoskip and samplerows appropriately.
		 */

		/* Fetch some rows. Tuples have to be stored in anl_cxt */
		numrows = remote_cursor_fetch_data(astate.cursor);

		/* Process whatever we got. */
		for (i = 0; i < numrows; i++)
			analyze_row_processor(i, &astate);

		/* Must be EOF if we didn't get all the rows requested. */
		if (numrows < fetch_size)
			break;
	}

	/* Close the cursor, just to be tidy. */
	remote_cursor_close(astate.cursor);

	/* We assume that we have no dead tuple. */
	*totaldeadrows = 0.0;

	/* We've retrieved all living tuples from foreign server. */
	*totalrows = astate.samplerows;

	/*
	 * Emit some interesting relation info
	 */
	ereport(elevel,
			(errmsg("\"%s\": table contains %.0f rows, %d rows in sample",
					RelationGetRelationName(relation),
					astate.samplerows,
					astate.numrows)));

	return astate.numrows;
}

/*
 * fdw_analyze_table
 *		Test whether analyzing this foreign table is supported
 */
bool
fdw_analyze_table(Relation relation, Oid serverid, BlockNumber *totalpages)
{
	TSConnection *conn;
	TSConnectionId id = remote_connection_id(serverid, relation->rd_rel->relowner);
	StringInfoData sql;
	AsyncRequest *volatile req = NULL;
	AsyncResponseResult *volatile rsp = NULL;

	conn = remote_dist_txn_get_connection(id, REMOTE_TXN_NO_PREP_STMT);

	/*
	 * Construct command to get page count for relation.
	 */
	initStringInfo(&sql);
	deparseAnalyzeSizeSql(&sql, relation);

	/* In what follows, do not risk leaking any PGresults. */
	PG_TRY();
	{
		PGresult *res;

		req = async_request_send(conn, sql.data);
		Assert(NULL != req);
		rsp = async_request_wait_any_result(req);
		Assert(NULL != rsp);
		res = async_response_result_get_pg_result(rsp);

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
			remote_result_elog(res, ERROR);

		if (PQntuples(res) != 1 || PQnfields(res) != 1)
			elog(ERROR, "unexpected result from analyze table query");

		*totalpages = strtoul(PQgetvalue(res, 0, 0), NULL, 10);

		async_response_result_close(rsp);
		rsp = NULL;
		pfree(req);
		req = NULL;
	}
	PG_CATCH();
	{
		if (NULL != req)
			pfree(req);

		if (NULL != rsp)
			async_response_result_close(rsp);

		PG_RE_THROW();
	}
	PG_END_TRY();

	return true;
}
