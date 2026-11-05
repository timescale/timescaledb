/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <nodes/pg_list.h>
#include <utils/jsonb.h>

#include "export.h"

typedef struct StoragePolicy
{
	int32 id;
	NameData name;
	Jsonb *policy;
} StoragePolicy;

/*
 * Find the enabled policy that applies to a new table named schema_name.table_name. An exact
 * match wins. Otherwise, of the regex policies matching both names, the one with the lowest
 * priority wins. Regexes are anchored, so they have to match the whole name.
 */
extern TSDLLEXPORT bool ts_storage_policy_find(const char *schema_name, const char *table_name,
											   StoragePolicy *policy);

/*
 * Validate a policy document and return the CREATE TABLE ... WITH options it stands for,
 * as tsdb.* DefElems plus the parts that have no WITH equivalent. Raises an error for anything that is not understood, as a misspelled
 * key must not silently become "no setting".
 */
typedef struct StoragePolicyOptions
{
	/* tsdb.* options for CREATE TABLE ... WITH */
	List *with_options;
	/* the age for the retention policy, or NULL for none. It has no WITH equivalent. */
	char *retention_after;
} StoragePolicyOptions;

extern TSDLLEXPORT StoragePolicyOptions *ts_storage_policy_to_options(const Jsonb *policy);
