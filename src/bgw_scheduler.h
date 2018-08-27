#ifndef TIMESCALEDB_BGW_SCHEDULER_H
#define TIMESCALEDB_BGW_SCHEDULER_H
#include <postgres.h>
#include <fmgr.h>
#include "compat.h"

extern Datum bgw_db_scheduler_main(PG_FUNCTION_ARGS);
#endif							/* TIMESCALEDB_BGW_SCHEDULER_H */
