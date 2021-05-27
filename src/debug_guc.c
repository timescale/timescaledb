/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <nodes/pathnodes.h>
#include <utils/builtins.h>
#include <utils/guc.h>
#include <utils/varlena.h>
#include <fmgr.h>

#include "debug_guc.h"

TSDLLEXPORT DebugOptimizerFlags ts_debug_optimizer_flags;

enum DebugFlag
{
	DEBUG_FLAG_UPPER,
	DEBUG_FLAG_REL
};

struct DebugFlagDef
{
	const char *name;
	enum DebugFlag flag;
};

static struct DebugFlagDef g_flag_names[] = {
	/* Show paths considered when planning a query. */
	{ "show_upper_paths", DEBUG_FLAG_UPPER },
	/* Show relations generated when planning a query. */
	{ "show_rel_pathlist", DEBUG_FLAG_REL },
};

static unsigned long
get_show_upper_mask(const char *paths, size_t paths_len)
{
	unsigned long mask = 0UL;
	const char *beg = paths;

	/* We can return early if there are no flags provided */
	if (paths_len == 0)
		return mask;

	while (true)
	{
		const char *const maybe_end = strchr(beg, ',');
		const char *const end = maybe_end == NULL ? paths + paths_len : maybe_end;
		const size_t len = end - beg;
		if (len > 0)
		{
			/* For each of the checks below, we check the provided string and
			 * allow a prefix to the full name, so "fin" will match
			 * "final". We have special support for "*" to denote setting all
			 * stages. */
			if (strncmp(beg, "*", len) == 0)
				mask |= ~0UL;
			else if (strncmp(beg, "setop", len) == 0)
				mask |= STAGE_SETOP;
			else if (strncmp(beg, "partial_group_agg", len) == 0)
				mask |= STAGE_PARTIAL_GROUP_AGG;
			else if (strncmp(beg, "group_agg", len) == 0)
				mask |= STAGE_GROUP_AGG;
			else if (strncmp(beg, "window", len) == 0)
				mask |= STAGE_WINDOW;
			else if (strncmp(beg, "distinct", len) == 0)
				mask |= STAGE_DISTINCT;
			else if (strncmp(beg, "ordered", len) == 0)
				mask |= STAGE_ORDERED;
			else if (strncmp(beg, "final", len) == 0)
				mask |= STAGE_FINAL;
			else
			{
				char buf[20] = { 0 };
				char *ptr;
				strncpy(buf, beg, sizeof(buf));

				/* If the path name was long, make it clear that it is
				 * incomplete in the printout */
				if (buf[19] != '\0')
				{
					buf[19] = '\0';
					buf[18] = '.';
					buf[17] = '.';
					buf[16] = '.';
				}

				/* Terminate the path if it is followed by a comma */
				ptr = strchr(buf, ',');
				if (ptr)
					*ptr = '\0';

				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg_internal("unrecognized flag option \"%s\"", buf)));
			}
		}
		if (maybe_end == NULL)
			break;
		beg = maybe_end + 1;
	}
	return mask;
}

static bool
set_debug_flag(const char *flag_string, size_t length, DebugOptimizerFlags *flags)
{
	int i;
	char *end;
	size_t flag_length;

	if ((end = strchr(flag_string, '=')) != NULL)
	{
		Assert(end - flag_string >= 0);
		flag_length = end - flag_string;
	}
	else
	{
		flag_length = length;
	}

	for (i = 0; i < sizeof(g_flag_names) / sizeof(*g_flag_names); ++i)
		if (strncmp(g_flag_names[i].name, flag_string, flag_length) == 0)
			switch (g_flag_names[i].flag)
			{
				case DEBUG_FLAG_UPPER:
					/* show_upper was missing flags for the mask */
					if (end == NULL)
						return false;
					flags->show_upper = get_show_upper_mask(end + 1, length - flag_length - 1);
					return true;
				case DEBUG_FLAG_REL:
					flags->show_rel = true;
					return true;
			}
	return false;
}

static bool
parse_optimizer_flags(const char *string, DebugOptimizerFlags *flags)
{
	char *rawname;
	List *namelist;
	ListCell *cell;
	DebugOptimizerFlags local_flags = { 0 };

	Assert(string && flags);

	if (strlen(string) == 0)
		return true;

	rawname = pstrdup(string);
	if (!SplitIdentifierString(rawname, ':', &namelist))
	{
		GUC_check_errdetail("Invalid flag string syntax \"%s\".", rawname);
		GUC_check_errhint("The flags string should be a list of colon-separated identifiers.");
		pfree(rawname);
		list_free(namelist);
		return false;
	}

	foreach (cell, namelist)
	{
		char *flag_string = (char *) lfirst(cell);
		if (!set_debug_flag(flag_string, strlen(flag_string), &local_flags))
		{
			GUC_check_errdetail("Unrecognized flag setting \"%s\".", flag_string);
			GUC_check_errhint("Allowed values are: show_upper_paths show_rel_pathlist");
			pfree(rawname);
			list_free(namelist);
			return false;
		}
	}

	*flags = local_flags;

	pfree(rawname);
	list_free(namelist);
	return true;
}

static bool
debug_optimizer_flags_check(char **newval, void **extra, GucSource source)
{
	DebugOptimizerFlags flags;

	Assert(newval);

	if (*newval)
		return parse_optimizer_flags(*newval, &flags);
	return true;
}

static void
debug_optimizer_flags_assign(const char *newval, void *extra)
{
	if (newval == NULL)
	{
		ts_debug_optimizer_flags.show_rel = false;
		ts_debug_optimizer_flags.show_upper = 0;
		return;
	}

	if (!parse_optimizer_flags(newval, &ts_debug_optimizer_flags))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("cannot parse \"%s\" as debug optimizer flags", newval)));
}

void
ts_debug_init(void)
{
	static char *debug_optimizer_flags_string = NULL;
	DefineCustomStringVariable("timescaledb.debug_optimizer_flags",
							   "List of optimizer debug flags",
							   "A list of flags for configuring the optimizer debug output.",
							   &debug_optimizer_flags_string,
							   NULL,
							   PGC_USERSET,
							   GUC_LIST_INPUT,
							   /* check_hook= */ debug_optimizer_flags_check,
							   /* assign_hook= */ debug_optimizer_flags_assign,
							   /* show_hook= */ NULL);
}
