/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_FDW_SCAN_EXEC_H
#define TIMESCALEDB_TSL_FDW_SCAN_EXEC_H

#include <postgres.h>
#include <utils/rel.h>
#include <utils/palloc.h>
#include <fmgr.h>
#include <access/htup.h>

/*
 * Execution state of a foreign scan using timescaledb_fdw.
 */
typedef struct TsFdwScanState
{
	Relation rel;								 /* relcache entry for the foreign table. NULL
												  * for a foreign join scan. */
	TupleDesc tupdesc;							 /* tuple descriptor of scan */
	struct AttConvInMetadata *att_conv_metadata; /* attribute datatype conversion metadata */

	/* extracted fdw_private data */
	char *query;		   /* text of SELECT command */
	List *retrieved_attrs; /* list of retrieved attribute numbers */

	/* for remote query execution */
	struct TSConnection *conn;  /* connection for the scan */
	unsigned int cursor_number; /* quasi-unique ID for my cursor */
	bool cursor_exists;			/* have we created the cursor? */
	int num_params;				/* number of parameters passed to query */
	FmgrInfo *param_flinfo;		/* output conversion functions for them */
	List *param_exprs;			/* executable expressions for param values */
	const char **param_values;  /* textual values of query parameters */

	/* for storing result tuples */
	HeapTuple *tuples; /* array of currently-retrieved tuples */
	int num_tuples;	/* # of tuples in array */
	int next_tuple;	/* index of next one to return */

	/* batch-level state, for optimizing rewinds and avoiding useless fetch */
	int fetch_ct_2;   /* Min(# of fetches done, 2) */
	bool eof_reached; /* true if last fetch reached EOF */

	/* working memory contexts */
	MemoryContext batch_cxt; /* context holding current batch of tuples */
	MemoryContext temp_cxt;  /* context for per-tuple temporary data */

	int fetch_size; /* number of tuples per fetch */
	int row_counter;
} TsFdwScanState;

extern void fdw_scan_init(ScanState *ss, TsFdwScanState *fsstate, Bitmapset *scanrelids,
						  List *fdw_private, List *fdw_exprs, int eflags);
extern TupleTableSlot *fdw_scan_iterate(ScanState *ss, TsFdwScanState *fsstate);
extern void fdw_scan_rescan(ScanState *ss, TsFdwScanState *fsstate);
extern void fdw_scan_end(TsFdwScanState *fsstate);
extern void fdw_scan_explain(ScanState *ss, List *fdw_private, ExplainState *es);

#endif /* TIMESCALEDB_TSL_FDW_SCAN_EXEC_H */
