#ifndef IOBEAMDB_METADATA_QUERIES_H
#define IOBEAMDB_METADATA_QUERIES_H

#include "iobeamdb.h"
#include "utils/lsyscache.h"
#include "catalog/namespace.h"
#include "executor/spi.h"
#include "fmgr.h"

#define OPEN_START_TIME -1
#define OPEN_END_TIME PG_INT64_MAX

typedef struct epoch_and_partitions_set epoch_and_partitions_set;

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

extern chunk_row *fetch_chunk_row(chunk_row *entry, int32 partition_id, int64 time_pt, bool lock);

extern crn_set *fetch_crn_set(crn_set *entry, int32 chunk_id);

#endif   /* IOBEAMDB_METADATA_QUERIES_H */
