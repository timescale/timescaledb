#ifndef TIMESCALEDB_PARTITIONING_H
#define TIMESCALEDB_PARTITIONING_H

#define KEYSPACE_PT_NO_PARTITIONING -1

#include <postgres.h>
#include <access/attnum.h>
#include <access/htup.h>
#include <fmgr.h>

#define OPEN_START_TIME -1
#define OPEN_END_TIME PG_INT64_MAX

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
} PartitioningFunc;


typedef struct PartitioningInfo
{
	char		column[NAMEDATALEN];
	AttrNumber	column_attnum;
	PartitioningFunc partfunc;
} PartitioningInfo;


extern PartitioningInfo *partitioning_info_create(int num_partitions,
												  const char *schema,
												  const char *partfunc,
												  const char *partcol,
												  Oid relid);
	
extern int32		partitioning_func_apply(PartitioningInfo *pinfo, Datum value);
extern int32		partitioning_func_apply_tuple(PartitioningInfo *pinfo, HeapTuple tuple, TupleDesc desc);

#endif   /* TIMESCALEDB_PARTITIONING_H */
