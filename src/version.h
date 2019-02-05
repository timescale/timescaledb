/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_VERSION_H
#define TIMESCALEDB_VERSION_H

#include <postgres.h>

#define VERSION_INFO_LEN 128

typedef struct VersionOSInfo
{
	char sysname[VERSION_INFO_LEN];
	char version[VERSION_INFO_LEN];
	char release[VERSION_INFO_LEN];
	char pretty_version[VERSION_INFO_LEN];
	bool has_pretty_version;
} VersionOSInfo;

extern bool ts_version_get_os_info(VersionOSInfo *info);

#endif /* TIMESCALEDB_VERSION_H */
