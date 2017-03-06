#ifndef TIMESCALEDB_INSERT_H
#define TIMESCALEDB_INSERT_H

#include "fmgr.h"
/* exported pg functions */
extern Datum insert_trigger_on_copy_table_c(PG_FUNCTION_ARGS);

extern Oid	create_copy_table(int32 hypertable_id, Oid root_oid);

#endif   /* TIMESCALEDB_INSERT_H */
