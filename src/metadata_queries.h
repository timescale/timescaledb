#ifndef TIMESCALEDB_METADATA_QUERIES_H
#define TIMESCALEDB_METADATA_QUERIES_H

#include <postgres.h>

typedef struct Chunk Chunk;
typedef struct Hyperspace Hyperspace;
typedef struct Point Point;

extern Chunk *spi_chunk_create(Hyperspace *hs, Point *p);

#endif   /* TIMESCALEDB_METADATA_QUERIES_H */
