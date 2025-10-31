/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <nodes/parsenodes.h>
#include <nodes/pathnodes.h>
#include <nodes/pg_list.h>
#include <parser/parsetree.h>

#include "chunk.h"
#include "export.h"
#include "guc.h"
#include "hypertable.h"

typedef struct Chunk Chunk;
typedef struct Hypertable Hypertable;

void _executor_init(void);
void _executor_fini(void);
