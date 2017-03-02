#ifndef IOBEAMDB_HYPERTABLE_CACHE_H
#define IOBEAMDB_HYPERTABLE_CACHE_H

#include <postgres.h>

typedef struct hypertable_basic_info hypertable_basic_info;
typedef struct epoch_and_partitions_set epoch_and_partitions_set;
typedef struct partition_info partition_info;

#define HYPERTABLE_CACHE_INVAL_PROXY_TABLE "cache_inval_hypertable"
#define HYPERTABLE_CACHE_INVAL_PROXY_OID                                \
    get_relname_relid(HYPERTABLE_CACHE_INVAL_PROXY_TABLE, CACHE_INVAL_PROXY_SCHEMA_OID)

#define MAX_EPOCHS_PER_HYPERTABLE_CACHE_ENTRY 20

typedef struct hypertable_cache_entry
{
    int32       id;
    char        time_column_name[NAMEDATALEN];
    Oid         time_column_type;
    int         num_epochs;
    /* Array of epoch_and_partitions_set*. Order by start_time */
    epoch_and_partitions_set *epochs[MAX_EPOCHS_PER_HYPERTABLE_CACHE_ENTRY];
} hypertable_cache_entry;

hypertable_cache_entry *hypertable_cache_get(int32 hypertable_id);

epoch_and_partitions_set *
hypertable_cache_get_partition_epoch(hypertable_cache_entry *hce, int64 time_pt, Oid relid);

void invalidate_hypertable_cache_callback(void);

void _hypertable_cache_init(void);
void _hypertable_cache_fini(void);

#endif   /* IOBEAMDB_HYPERTABLE_CACHE_H */
