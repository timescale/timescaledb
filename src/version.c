/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#include <postgres.h>
#include <string.h>
#include <access/htup_details.h>
#include <utils/builtins.h>
#include <funcapi.h>
#include <fmgr.h>

#include "fmgr.h"
#include "compat.h"
#include "gitcommit.h"
#include "version.h"
#include "config.h"

#define STR_EXPAND(x) #x
#define STR(x) STR_EXPAND(x)

void
version_get_info(VersionInfo *vinfo)
{
	memset(vinfo, 0, sizeof(VersionInfo));
	vinfo->version[0] = strtol(TIMESCALEDB_MAJOR_VERSION, NULL, 10);
	vinfo->version[1] = strtol(TIMESCALEDB_MINOR_VERSION, NULL, 10);
	vinfo->version[2] = strtol(TIMESCALEDB_PATCH_VERSION, NULL, 10);

	if (strlen(TIMESCALEDB_MOD_VERSION) > 0)
	{
		StrNCpy(vinfo->version_mod, TIMESCALEDB_MOD_VERSION, sizeof(vinfo->version_mod));
		vinfo->has_version_mod = true;
	}
}

/*
 * Compare two versions.
 *
 * It returns an integer less than, equal to, or greater than zero if version
 * v1 is found, respectively, to be less than, to match, or be greater than
 * version v2.
 */
int
version_cmp(VersionInfo *v1, VersionInfo *v2)
{
	int			i;

	for (i = 0; i < 3; i++)
	{
		if (v1->version[i] > v2->version[i])
			return 1;

		if (v1->version[i] < v2->version[i])
			return -1;
	}

	/*
	 * Note that the version mod signifies a pre-release version, so having a
	 * version mod is "less" than not having one with otherwise identical
	 * versions
	 */
	if (v1->has_version_mod && !v2->has_version_mod)
		return -1;

	if (!v1->has_version_mod && v2->has_version_mod)
		return 1;

	/* Compare the version mod lexicographically */
	if (v1->has_version_mod && v2->has_version_mod)
		return strncmp(v1->version_mod, v2->version_mod, sizeof(v1->version_mod));

	return 0;
}

bool
version_parse(const char *version, VersionInfo *result)
{
	size_t		version_len = strlen(version);
	int			fields_parsed = 0;
	int			i = 0;
	int			parse_length[4] = {0};

	memset(result, 0, sizeof(VersionInfo));

	/*
	 * a version is a string of the form
	 *
	 * <number>[.<number>[.<number>[-<string>]]]
	 *
	 * this corresponds to the format-string
	 *
	 * "%lu.%lu.%lu-%<VERSION_INFO_LEN>s"
	 *
	 * after parsing each field in the version-string, we output the number of
	 * bytes currently parsed using %n, a version-string is valid if all of
	 * the bytes of the string were parsable using the above grammar.
	 */
	fields_parsed = sscanf(version, "%lu%n.%lu%n.%lu%n-%" STR(VERSION_INFO_LEN) "s%n",
						   &result->version[0], &parse_length[0],
						   &result->version[1], &parse_length[1],
						   &result->version[2], &parse_length[2],
						   result->version_mod, &parse_length[3]);

	/*
	 * sscanf is allowed to return EOF if no parses succeed, so make sure
	 * fields_parsed is between 1 and 4;
	 */
	if (fields_parsed <= 0 || fields_parsed > 4)
		return false;

	result->has_version_mod = fields_parsed > 3;

	result->version_mod[VERSION_INFO_LEN - 1] = '\0';

	for (i = 0; i < VERSION_INFO_LEN; i++)
		if (!isprint(result->version_mod[i]))
			result->version_mod[i] = '\0';

	return ((size_t) parse_length[fields_parsed - 1]) == version_len;
}

TS_FUNCTION_INFO_V1(ts_version_get_info);

Datum
ts_version_get_info(PG_FUNCTION_ARGS)
{
	VersionInfo info;
	TupleDesc	tupdesc;
	Datum		values[4];
	bool		nulls[4] = {false};
	HeapTuple	tuple;

	version_get_info(&info);

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	values[0] = Int32GetDatum((int32) info.version[0]);
	values[1] = Int32GetDatum((int32) info.version[1]);
	values[2] = Int32GetDatum((int32) info.version[2]);

	if (info.has_version_mod)
		values[3] = CStringGetTextDatum(info.version_mod);
	else
		nulls[3] = true;

	tuple = heap_form_tuple(tupdesc, values, nulls);

	return HeapTupleGetDatum(tuple);
}

const char *git_commit = STR(EXT_GIT_COMMIT);

TS_FUNCTION_INFO_V1(ts_get_git_commit);

Datum
ts_get_git_commit(PG_FUNCTION_ARGS)
{
	size_t		var_size = VARHDRSZ + strlen(git_commit);
	text	   *version_text = (text *) palloc(var_size);

	SET_VARSIZE(version_text, var_size);

	memcpy((void *) VARDATA(version_text),
		   (void *) git_commit,
		   var_size - VARHDRSZ);

	PG_RETURN_TEXT_P(version_text);
}

#ifdef WIN32

#include <Windows.h>

bool
version_get_os_info(VersionOSInfo *info)
{
	DWORD		bufsize;
	void	   *buffer;
	VS_FIXEDFILEINFO *vinfo = NULL;
	UINT		vinfo_len = 0;

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

bool
version_get_os_info(VersionOSInfo *info)
{
	/* Get the OS name  */
	struct utsname os_info;

	uname(&os_info);

	memset(info, 0, sizeof(VersionOSInfo));
	strncpy(info->sysname, os_info.sysname, VERSION_INFO_LEN - 1);
	strncpy(info->version, os_info.version, VERSION_INFO_LEN - 1);
	strncpy(info->release, os_info.release, VERSION_INFO_LEN - 1);

	return true;
}
#else
bool
version_get_os_info(VersionOSInfo *info)
{
	memset(info, 0, sizeof(VersionOSInfo));
	return false;
}
#endif							/* WIN32 */

TS_FUNCTION_INFO_V1(ts_get_os_info);

Datum
ts_get_os_info(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		values[3];
	bool		nulls[3] = {false};
	HeapTuple	tuple;
	VersionOSInfo info;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	if (version_get_os_info(&info))
	{
		values[0] = CStringGetTextDatum(info.sysname);
		values[1] = CStringGetTextDatum(info.version);
		values[2] = CStringGetTextDatum(info.release);
	}
	else
		memset(nulls, true, sizeof(nulls));

	tuple = heap_form_tuple(tupdesc, values, nulls);

	return HeapTupleGetDatum(tuple);
}
