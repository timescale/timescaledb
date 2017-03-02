/* -*- Mode: C; tab-width: 4; indent-tabs-mode: t; c-basic-offset: 4 -*- */
#ifndef IOBEAMDB_H
#define IOBEAMDB_H

#include <postgres.h>

#define IOBEAMDB_CATALOG_SCHEMA         "_iobeamdb_catalog"
#define IOBEAMDB_INTERNAL_SCHEMA        "_iobeamdb_internal"
#define IOBEAMDB_HYPERTABLE_TABLE       "hypertable"

typedef struct Node Node;

bool IobeamLoaded(void);

char *copy_table_name(int32 hypertable_id);

#endif /* IOBEAMDB_H */
