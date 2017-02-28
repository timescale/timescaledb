#ifndef IOBEAMDB_CHUNK_CACHE_H
#define IOBEAMDB_CHUNK_CACHE_H

#include <postgres.h>
#include <executor/spi.h>

#define CHUNK_CACHE_INVAL_PROXY_TABLE "cache_inval_chunk"
#define CHUNK_CACHE_INVAL_PROXY_OID										\
	get_relname_relid(CHUNK_CACHE_INVAL_PROXY_TABLE, CACHE_INVAL_PROXY_SCHEMA_OID)

typedef struct hypertable_cache_entry hypertable_cache_entry;
typedef struct epoch_and_partitions_set epoch_and_partitions_set;
typedef struct Partition Partition;
typedef struct chunk_row chunk_row;

typedef struct chunk_cache_entry
{
	int32		id;
	chunk_row  *chunk;
	SPIPlanPtr	move_from_copyt_plan;
} chunk_cache_entry;

extern chunk_cache_entry *get_chunk_cache_entry(hypertable_cache_entry *hci, epoch_and_partitions_set *pe_entry,
												Partition *part, int64 time_pt, bool lock);

extern void invalidate_chunk_cache_callback(void);

extern void _chunk_cache_init(void);
extern void _chunk_cache_fini(void);

#endif   /* IOBEAMDB_CHUNK_CACHE_H */
