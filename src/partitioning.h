#ifndef IOBEAMDB_PARTITIONING_H
#define IOBEAMDB_PARTITIONING_H

#include <postgres.h>
#include <access/attnum.h>
#include <fmgr.h>

typedef struct Partition
{
    int32 id;
    int16 keyspace_start;
    int16 keyspace_end;
} Partition;

typedef struct PartitioningFunc
{
    char schema[NAMEDATALEN];
    char name[NAMEDATALEN];

	/* Function manager info to call the function to convert a row's
	 * partitioning column value to a text string */
    FmgrInfo textfunc_fmgr;
	
	/* Function manager info to call the partitioning function on the
	   partitioning column's text representation */
    FmgrInfo func_fmgr;
    int32 modulos;
} PartitioningFunc;


typedef struct PartitioningInfo
{
    char column[NAMEDATALEN];
    AttrNumber column_attnum;
	PartitioningFunc partfunc;
} PartitioningInfo;

typedef struct epoch_and_partitions_set
{
	int32		 id;
	int32		 hypertable_id;
	int64		 start_time;
	int64		 end_time;
	PartitioningInfo *partitioning;
    int16        num_partitions;
	Partition    partitions[0];
} epoch_and_partitions_set;

typedef struct epoch_and_partitions_set epoch_and_partitions_set;

epoch_and_partitions_set *partition_epoch_scan(int32 hypertable_id, int64 timepoint, Oid relid);
int16 partitioning_func_apply(PartitioningFunc *pf, Datum value);

Partition *partition_epoch_get_partition(epoch_and_partitions_set *epoch, int16 keyspace_pt);
int16 * partition_epoch_get_partition_end_times(epoch_and_partitions_set *epoch);
#endif /* IOBEAMDB_PARTITIONING_H */
