/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TIMESCALEDB_INDEXING_H
#define TIMESCALEDB_INDEXING_H

#include <postgres.h>
#include <nodes/pg_list.h>
#include <nodes/parsenodes.h>

#include "dimension.h"

extern void indexing_verify_columns(Hyperspace *hs, List *indexelems);
extern void indexing_verify_index(Hyperspace *hs, IndexStmt *stmt);
extern void indexing_verify_indexes(Hypertable *ht);
extern void indexing_create_default_indexes(Hypertable *ht);

#endif							/* TIMESCALEDB_INDEXING_H */
