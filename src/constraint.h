#ifndef TIMESCALEDB_CONSTRAINT_H
#define TIMESCALEDB_CONSTRAINT_H

#include <postgres.h>
#include <nodes/parsenodes.h>

#include "chunk.h"

extern Constraint *constraint_make_dimension_check(Chunk *chunk, Dimension *dim, ColumnDef *coldef);

#endif   /* TIMESCALEDB_CONSTRAINT_H */
