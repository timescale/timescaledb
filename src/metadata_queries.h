#ifndef TIMESCALEDB_METADATA_QUERIES_H
#define TIMESCALEDB_METADATA_QUERIES_H

#include <postgres.h>
#include "dimension.h"

typedef struct Chunk Chunk;

extern Chunk *spi_chunk_get_or_create(Hyperspace *hs, Point *p);

extern Chunk *spi_chunk_create(Hyperspace *hs, Point *p);

#endif   /* TIMESCALEDB_METADATA_QUERIES_H */
