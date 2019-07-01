/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_FDW_OPTION_H
#define TIMESCALEDB_TSL_FDW_OPTION_H

#include <postgres.h>

extern void option_validate(List *options_list, Oid catalog);
extern List *option_extract_extension_list(const char *extensionsString, bool warn_on_missing);
extern bool option_get_from_options_list_int(List *options, const char *optionname, int *value);

#endif /* TIMESCALEDB_TSL_FDW_OPTION_H */
