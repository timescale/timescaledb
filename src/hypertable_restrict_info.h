/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TIMESCALEDB_HYPERTABLE_RESTRICT_INFO_H
#define TIMESCALEDB_HYPERTABLE_RESTRICT_INFO_H

#include "hypertable.h"


/* HypertableRestrictInfo represents restrictions on a hypertable. It uses
 * range exclusion logic to figure out which chunks can match the description */
typedef struct HypertableRestrictInfo HypertableRestrictInfo;

extern HypertableRestrictInfo *ts_hypertable_restrict_info_create(RelOptInfo *rel, Hypertable *ht);

/* Add restrictions based on a List of RestrictInfo */
extern void ts_hypertable_restrict_info_add(HypertableRestrictInfo *hri, PlannerInfo *root, List *base_restrict_infos);

/* Some restrictions were added */
extern bool ts_hypertable_restrict_info_has_restrictions(HypertableRestrictInfo *hri);

/* Get a list of chunk oids for chunks whose constraints match the restriction clauses */
extern List *ts_hypertable_restrict_info_get_chunk_oids(HypertableRestrictInfo *hri, Hypertable *ht, LOCKMODE lockmode);

#endif							/* TIMESCALEDB_HYPERTABLE_RESTRICT_INFO_H */
