#ifndef TIMESCALEDB_METADATA_QUERIES_H
#define TIMESCALEDB_METADATA_QUERIES_H

#include <postgres.h>

typedef struct Chunk Chunk;
typedef struct Hyperspace Hyperspace;
typedef struct Point Point;
typedef struct Hypertable Hypertable;

extern Chunk *spi_chunk_create(Hyperspace *hs, Point *p);
extern void spi_hypertable_rename(Hypertable *ht, char *new_schema_name, char *new_table_name);

#endif   /* TIMESCALEDB_METADATA_QUERIES_H */
