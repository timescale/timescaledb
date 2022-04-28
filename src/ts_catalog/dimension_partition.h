/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_DIMENSION_PARTITION_H
#define TIMESCALEDB_DIMENSION_PARTITION_H

#include <postgres.h>

#include "scan_iterator.h"

typedef struct DimensionPartition
{
	int32 dimension_id;
	int64 range_start;
	int64 range_end;
	List *data_nodes;
} DimensionPartition;

typedef struct DimensionPartitionInfo
{
	unsigned int num_partitions;
	DimensionPartition **partitions;
} DimensionPartitionInfo;

extern DimensionPartitionInfo *ts_dimension_partition_info_get(int32 dimension_id);
extern const DimensionPartition *ts_dimension_partition_find(const DimensionPartitionInfo *dpi,
															 int64 coord);
extern TSDLLEXPORT DimensionPartitionInfo *
ts_dimension_partition_info_recreate(int32 dimension_id, unsigned int num_partitions,
									 List *data_nodes, int replication_factor);
extern void ts_dimension_partition_info_delete(int dimension_id);

#endif /* TIMESCALEDB_DIMENSION_PARTITION_H */
