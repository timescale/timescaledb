#ifndef TIMESCALEDB_PARSE_REWRITE_H
#define TIMESCALEDB_PARSE_REWRITE_H

#include <postgres.h>
#include <nodes/parsenodes.h>

typedef struct Hypertable Hypertable;

extern void parse_rewrite_query(ParseState *pstate, Query *parse, Hypertable *ht);

#endif   /* TIMESCALEDB_PARSE_REWRITE_H */
