#ifndef TIMESCALEDB_VERSION_H
#define TIMESCALEDB_VERSION_H

#define VERSION_OS_INFO_LEN 128

typedef struct VersionOSInfo
{
	char		sysname[VERSION_OS_INFO_LEN];
	char		version[VERSION_OS_INFO_LEN];
	char		release[VERSION_OS_INFO_LEN];
} VersionOSInfo;

extern bool version_get_os_info(VersionOSInfo *info);

#endif							/* TIMESCALEDB_VERSION_H */
