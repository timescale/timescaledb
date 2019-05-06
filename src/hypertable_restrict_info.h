/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_HYPERTABLE_RESTRICT_INFO_H
#define TIMESCALEDB_HYPERTABLE_RESTRICT_INFO_H

#include "hypertable.h"

/* HypertableRestrictInfo represents restrictions on a hypertable. It uses
 * range exclusion logic to figure out which chunks can match the description */
typedef struct HypertableRestrictInfo HypertableRestrictInfo;

extern HypertableRestrictInfo *ts_hypertable_restrict_info_create(RelOptInfo *rel, Hypertable *ht);

/* Add restrictions based on a List of RestrictInfo */
extern void ts_hypertable_restrict_info_add(HypertableRestrictInfo *hri, PlannerInfo *root,
											List *base_restrict_infos);

/* Some restrictions were added */
extern bool ts_hypertable_restrict_info_has_restrictions(HypertableRestrictInfo *hri);

/* Get a list of chunk oids for chunks whose constraints match the restriction clauses */
extern List *ts_hypertable_restrict_info_get_chunk_oids(HypertableRestrictInfo *hri, Hypertable *ht,
														LOCKMODE lockmode);

extern List *ts_hypertable_restrict_info_get_chunk_oids_ordered(HypertableRestrictInfo *hri,
																Hypertable *ht, LOCKMODE lockmode,
																List **nested_oids, bool reverse);

#endif /* TIMESCALEDB_HYPERTABLE_RESTRICT_INFO_H */
