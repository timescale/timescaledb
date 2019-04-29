/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifndef TIMESCALEDB_TSL_HYPERTABLE
#define TIMESCALEDB_TSL_HYPERTABLE

#include <hypertable.h>

#include "catalog.h"
#include "dimension.h"
#include "interval.h"

extern Datum hypertable_valid_ts_interval(PG_FUNCTION_ARGS);
extern void hypertable_make_distributed(Hypertable *ht, ArrayType *servers);

List *hypertable_assign_servers(int32 hypertable_id, List *servers);
extern List *hypertable_server_array_to_list(ArrayType *serverarr);

#endif /* _TIMESCALEDB_TSL_HYPERTABLE_H */
