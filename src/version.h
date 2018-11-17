/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TIMESCALEDB_VERSION_H
#define TIMESCALEDB_VERSION_H

#include <postgres.h>

#define VERSION_INFO_LEN 128

typedef struct VersionOSInfo
{
	char		sysname[VERSION_INFO_LEN];
	char		version[VERSION_INFO_LEN];
	char		release[VERSION_INFO_LEN];
} VersionOSInfo;

extern bool version_get_os_info(VersionOSInfo *info);

#endif							/* TIMESCALEDB_VERSION_H */
