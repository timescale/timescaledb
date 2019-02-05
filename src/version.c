/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <string.h>
#include <access/htup_details.h>
#include <utils/builtins.h>
#include <funcapi.h>
#include <fmgr.h>
#include <storage/fd.h>

#include "fmgr.h"
#include "compat.h"
#include "gitcommit.h"
#include "version.h"
#include "config.h"

#define STR_EXPAND(x) #x
#define STR(x) STR_EXPAND(x)

static const char *git_commit = STR(EXT_GIT_COMMIT);

TS_FUNCTION_INFO_V1(ts_get_git_commit);

Datum
ts_get_git_commit(PG_FUNCTION_ARGS)
{
	size_t var_size = VARHDRSZ + strlen(git_commit);
	text *version_text = (text *) palloc(var_size);

	SET_VARSIZE(version_text, var_size);

	memcpy((void *) VARDATA(version_text), (void *) git_commit, var_size - VARHDRSZ);

	PG_RETURN_TEXT_P(version_text);
}

#ifdef WIN32

#include <Windows.h>

bool
ts_version_get_os_info(VersionOSInfo *info)
{
	DWORD bufsize;
	void *buffer;
	VS_FIXEDFILEINFO *vinfo = NULL;
	UINT vinfo_len = 0;

	memset(info, 0, sizeof(VersionOSInfo));

	bufsize = GetFileVersionInfoSizeA(TEXT("kernel32.dll"), NULL);

	if (bufsize == 0)
		return false;

	buffer = palloc(bufsize);

	if (!GetFileVersionInfoA(TEXT("kernel32.dll"), 0, bufsize, buffer))
		goto error;

	if (!VerQueryValueA(buffer, TEXT("\\"), &vinfo, &vinfo_len))
		goto error;

	snprintf(info->sysname, VERSION_INFO_LEN - 1, "Windows");
	snprintf(info->version, VERSION_INFO_LEN - 1, "%u", HIWORD(vinfo->dwProductVersionMS));
	snprintf(info->release, VERSION_INFO_LEN - 1, "%u", LOWORD(vinfo->dwProductVersionMS));

	pfree(buffer);

	return true;
error:
	pfree(buffer);

	return false;
}

#elif defined(UNIX)

#include <sys/utsname.h>

#define OS_RELEASE_FILE "/etc/os-release"
#define MAX_READ_LEN 1024

#define NAME_FIELD "PRETTY_NAME=\""

static bool
get_pretty_version(char *pretty_version)
{
	FILE *version_file;
	char *contents = palloc(MAX_READ_LEN);
	size_t bytes_read;
	bool got_pretty_version = false;
	int i;

	memset(pretty_version, '\0', VERSION_INFO_LEN);

	/* we cannot use pg_read_file because it doesn't allow absolute paths */
	version_file = AllocateFile(OS_RELEASE_FILE, PG_BINARY_R);
	if (version_file == NULL)
		return false;

	fseeko(version_file, 0, SEEK_SET);

	bytes_read = fread(contents, 1, (size_t) MAX_READ_LEN, version_file);

	if (bytes_read <= 0)
		goto cleanup;

	if (bytes_read < MAX_READ_LEN)
		contents[bytes_read] = '\0';
	else
		contents[MAX_READ_LEN - 1] = '\0';

	contents = strstr(contents, NAME_FIELD);

	if (contents == NULL)
		goto cleanup;

	contents += sizeof(NAME_FIELD) - 1;

	for (i = 0; i < (VERSION_INFO_LEN - 1); i++)
	{
		char c = contents[i];

		if (c == '\0' || c == '\n' || c == '\r' || c == '"')
			break;

		pretty_version[i] = c;
	}

	got_pretty_version = true;

cleanup:
	FreeFile(version_file);
	return got_pretty_version;
}

bool
ts_version_get_os_info(VersionOSInfo *info)
{
	/* Get the OS name  */
	struct utsname os_info;

	uname(&os_info);

	memset(info, 0, sizeof(VersionOSInfo));
	strncpy(info->sysname, os_info.sysname, VERSION_INFO_LEN - 1);
	strncpy(info->version, os_info.version, VERSION_INFO_LEN - 1);
	strncpy(info->release, os_info.release, VERSION_INFO_LEN - 1);
	info->has_pretty_version = get_pretty_version(info->pretty_version);

	return true;
}
#else
bool
ts_version_get_os_info(VersionOSInfo *info)
{
	memset(info, 0, sizeof(VersionOSInfo));
	return false;
}
#endif /* WIN32 */

TS_FUNCTION_INFO_V1(ts_get_os_info);

Datum
ts_get_os_info(PG_FUNCTION_ARGS)
{
	TupleDesc tupdesc;
	Datum values[4];
	bool nulls[4] = { false };
	HeapTuple tuple;
	VersionOSInfo info;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	if (ts_version_get_os_info(&info))
	{
		values[0] = CStringGetTextDatum(info.sysname);
		values[1] = CStringGetTextDatum(info.version);
		values[2] = CStringGetTextDatum(info.release);
		if (info.has_pretty_version)
			values[3] = CStringGetTextDatum(info.pretty_version);
		else
			nulls[3] = true;
	}
	else
		memset(nulls, true, sizeof(nulls));

	tuple = heap_form_tuple(tupdesc, values, nulls);

	return HeapTupleGetDatum(tuple);
}
