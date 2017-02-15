#ifndef IOBEAMDB_METADATA_QUERIES_H
#define IOBEAMDB_METADATA_QUERIES_H

#include "iobeamdb.h"
#include "utils/lsyscache.h"
#include "catalog/namespace.h"
#include "executor/spi.h"
#include "fmgr.h"

#define OPEN_START_TIME -1
#define OPEN_END_TIME PG_INT64_MAX

typedef struct hypertable_basic_info
{
	int32		id;
	NameData	time_column_name;
	Oid			time_column_type;
	SPIPlanPtr	get_one_tuple_copyt_plan;
} hypertable_basic_info;

typedef struct partition_info
{
	int32		id;
	int16		keyspace_start;
	int16		keyspace_end;
} partition_info;

typedef struct epoch_and_partitions_set
{
	int32		id;
	int32		hypertable_id;
	int64		start_time;
	int64		end_time;
	Name		partitioning_func_schema;
	Name		partitioning_func;
	int32		partitioning_mod;
	Name		partitioning_column;
	AttrNumber	partitioning_column_attrnumber;
	Oid			partitioning_column_text_func;
	FmgrInfo   *partitioning_column_text_func_fmgr;
	FmgrInfo   *partition_func_fmgr;
	int			num_partitions;
	partition_info **partitions;
} epoch_and_partitions_set;

typedef struct chunk_row
{
	int32		id;
	int32		partition_id;
	int64		start_time;
	int64		end_time;
} chunk_row;

typedef struct crn_row
{
	NameData	schema_name;
	NameData	table_name;
} crn_row;

typedef struct crn_set
{
	int32		chunk_id;
	List	   *tables;
} crn_set;

/* utility func */
extern SPIPlanPtr prepare_plan(const char *src, int nargs, Oid *argtypes);


/* db access func */
extern epoch_and_partitions_set *fetch_epoch_and_partitions_set(epoch_and_partitions_set *entry,
							   int32 hypertable_id, int64 time_pt, Oid relid);

extern void free_epoch(epoch_and_partitions_set *epoch);

extern hypertable_basic_info *fetch_hypertable_info(hypertable_basic_info *entry, int32 hypertable_id);

extern chunk_row *fetch_chunk_row(chunk_row *entry, int32 partition_id, int64 time_pt, bool lock);

extern crn_set *fetch_crn_set(crn_set *entry, int32 chunk_id);

#endif   /* IOBEAMDB_METADATA_QUERIES_H */
