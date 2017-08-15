#ifndef TIMESCALEDB_METADATA_QUERIES_H
#define TIMESCALEDB_METADATA_QUERIES_H

#include <postgres.h>

typedef struct Hypertable Hypertable;

extern void spi_chunk_insert(int32 chunk_id, int32 hypertable_id, const char *schema_name, const char *table_name);
extern void spi_hypertable_rename(Hypertable *ht, const char *new_schema_name, const char *new_table_name);
extern void spi_hypertable_truncate(Hypertable *ht);

#endif   /* TIMESCALEDB_METADATA_QUERIES_H */
