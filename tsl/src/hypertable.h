/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_HYPERTABLE_H
#define TIMESCALEDB_TSL_HYPERTABLE_H

#include <hypertable.h>
#include "dimension.h"
#include "config.h"
#include "ts_catalog/catalog.h"

/* We cannot make use of more data nodes than we have slices in closed (space)
 * dimensions, and the value for number of slices is an int16. */
#define MAX_NUM_HYPERTABLE_DATA_NODES PG_INT16_MAX

extern void hypertable_make_distributed(Hypertable *ht, List *data_node_names);
extern List *hypertable_assign_data_nodes(int32 hypertable_id, List *nodes);
extern List *hypertable_get_and_validate_data_nodes(ArrayType *nodearr);
extern Datum hypertable_set_replication_factor(PG_FUNCTION_ARGS);

#endif /* TIMESCALEDB_TSL_HYPERTABLE_H */
