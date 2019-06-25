/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef _TIMESCALEDB_TSL_HYPERTABLE_H
#define _TIMESCALEDB_TSL_HYPERTABLE_H

#include <hypertable.h>

#include "catalog.h"

extern void hypertable_make_distributed(Hypertable *ht, ArrayType *data_nodes);

List *hypertable_assign_data_nodes(int32 hypertable_id, List *nodes);
extern List *hypertable_data_node_array_to_list(ArrayType *nodearr);

#endif /* _TIMESCALEDB_TSL_HYPERTABLE_H */
