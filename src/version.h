#ifndef TIMESCALEDB_VERSION_H
#define TIMESCALEDB_VERSION_H

#include <postgres.h>

#define VERSION_INFO_LEN 128
#define VERSION_PARTS 3

typedef struct VersionInfo
{
	long		version[VERSION_PARTS];
	char		version_mod[VERSION_INFO_LEN];
	bool		has_version_mod;
} VersionInfo;

typedef struct VersionOSInfo
{
	char		sysname[VERSION_INFO_LEN];
	char		version[VERSION_INFO_LEN];
	char		release[VERSION_INFO_LEN];
} VersionOSInfo;

extern void version_get_info(VersionInfo *vinfo);
extern int	version_cmp(VersionInfo *v1, VersionInfo *v2);
extern bool version_parse(const char *version, VersionInfo *result);

extern bool version_get_os_info(VersionOSInfo *info);

#endif							/* TIMESCALEDB_VERSION_H */
