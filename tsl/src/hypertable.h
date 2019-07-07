/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_HYPERTABLE_H
#define TIMESCALEDB_TSL_HYPERTABLE_H

#include <hypertable.h>

#include "catalog.h"
#include "dimension.h"
#include "interval.h"
#include "config.h"

extern Datum hypertable_valid_ts_interval(PG_FUNCTION_ARGS);

#if PG_VERSION_SUPPORTS_MULTINODE
extern void hypertable_make_distributed(Hypertable *ht, ArrayType *data_nodes);
extern List *hypertable_assign_data_nodes(int32 hypertable_id, List *nodes);
#endif /*  PG_VERSION_SUPPORTS_MULTINODE */

#endif /* TIMESCALEDB_TSL_HYPERTABLE_H */
