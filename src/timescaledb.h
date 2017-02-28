/* -*- Mode: C; tab-width: 4; indent-tabs-mode: t; c-basic-offset: 4 -*- */
#ifndef timescaledb_H
#define timescaledb_H

#include <postgres.h>

#define TIMESCALEDB_CATALOG_SCHEMA         "_timescaledb_catalog"
#define TIMESCALEDB_INTERNAL_SCHEMA        "_timescaledb_internal"
#define TIMESCALEDB_HYPERTABLE_TABLE       "hypertable"

typedef struct Node Node;

bool IobeamLoaded(void);

char *copy_table_name(int32 hypertable_id);

#endif /* timescaledb_H */
