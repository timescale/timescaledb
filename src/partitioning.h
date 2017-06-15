#ifndef TIMESCALEDB_PARTITIONING_H
#define TIMESCALEDB_PARTITIONING_H

#define KEYSPACE_PT_NO_PARTITIONING -1

#include <postgres.h>
#include <access/attnum.h>
#include <access/htup.h>
#include <fmgr.h>

#define OPEN_START_TIME -1
#define OPEN_END_TIME PG_INT64_MAX

typedef struct Partition
{
	int32		id;
	int32		index;			/* Index of this partition in the epoch's
								 * partition array */
	int16		keyspace_start;
	int16		keyspace_end;
} Partition;

typedef struct PartitioningFunc
{
	char		schema[NAMEDATALEN];
	char		name[NAMEDATALEN];

	/*
	 * Function manager info to call the function to convert a row's
	 * partitioning column value to a text string
	 */
	FmgrInfo	textfunc_fmgr;

	/*
	 * Function manager info to call the partitioning function on the
	 * partitioning column's text representation
	 */
	FmgrInfo	func_fmgr;
	int32		modulos;
} PartitioningFunc;


typedef struct PartitioningInfo
{
	char		column[NAMEDATALEN];
	AttrNumber	column_attnum;
	PartitioningFunc partfunc;
} PartitioningInfo;

typedef struct PartitionEpoch
{
	int32		id;
	int32		hypertable_id;
	int64		start_time;
	int64		end_time;
	PartitioningInfo *partitioning;
	int16		num_partitions;
	Partition	partitions[0];
} PartitionEpoch;


int16		partitioning_func_apply(PartitioningInfo *pinfo, Datum value);
int16		partitioning_func_apply_tuple(PartitioningInfo *pinfo, HeapTuple tuple, TupleDesc desc);

/*
PartitionEpoch *partition_epoch_scan(int32 hypertable_id, int64 timepoint, Oid relid);

Partition  *partition_epoch_get_partition(PartitionEpoch *epoch, int16 keyspace_pt);
void		partition_epoch_free(PartitionEpoch *epoch);

bool		partition_keyspace_pt_is_member(const Partition *part, const int16 keyspace_pt);
*/
#endif   /* TIMESCALEDB_PARTITIONING_H */
