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
#include <commands/explain.h>

#include "remote/data_fetcher.h"
#include "guc.h"

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
	struct TSConnection *conn;	/* connection for the scan */
	struct DataFetcher *fetcher;  /* fetches tuples from data node */
	int num_params;				  /* number of parameters passed to query */
	FmgrInfo *param_flinfo;		  /* output conversion functions for them */
	List *param_exprs;			  /* executable expressions for param values */
	const char **param_values;	/* textual values of query parameters */
	int fetch_size;				  /* number of tuples per fetch */
	DataFetcherType fetcher_type; /* the type of data fetcher to use  */
	int row_counter;
} TsFdwScanState;

extern void fdw_scan_init(ScanState *ss, TsFdwScanState *fsstate, Bitmapset *scanrelids,
						  List *fdw_private, List *fdw_exprs, int eflags);
extern TupleTableSlot *fdw_scan_iterate(ScanState *ss, TsFdwScanState *fsstate);
extern void fdw_scan_rescan(ScanState *ss, TsFdwScanState *fsstate);
extern void fdw_scan_end(TsFdwScanState *fsstate);
extern void fdw_scan_explain(ScanState *ss, List *fdw_private, ExplainState *es,
							 TsFdwScanState *fsstate);

extern DataFetcher *create_data_fetcher(ScanState *ss, TsFdwScanState *fsstate);

#ifdef TS_DEBUG

extern TimestampTz ts_current_timestamp_override_value;
/* Allow tests to specify the time to push down in place of now() */
extern void fdw_scan_debug_override_current_timestamp(TimestampTz time);
#endif

#endif /* TIMESCALEDB_TSL_FDW_SCAN_EXEC_H */
