#ifndef TIMESCALEDB_BGW_INTERFACE_H
#define TIMESCALEDB_BGW_INTERFACE_H

#include <postgres.h>
#include <fmgr.h>

/* This is where versioned-extension facing functions live. It shouldn't live anywhere else */

PGDLLEXPORT extern Datum ts_bgw_worker_reserve(PG_FUNCTION_ARGS);
PGDLLEXPORT extern Datum ts_bgw_worker_release(PG_FUNCTION_ARGS);
PGDLLEXPORT extern Datum ts_bgw_num_unreserved(PG_FUNCTION_ARGS);


#endif							/* TIMESCALEDB_BGW_INTERFACE_H */
