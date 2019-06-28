/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * This file contains source code that was copied and/or modified from
 * the PostgreSQL database, which is licensed under the open-source
 * PostgreSQL License. Please see the NOTICE at the top level
 * directory for a copy of the PostgreSQL License.
 */
/*-------------------------------------------------------------------------
 *
 * option.c
 *		  FDW option handling for timescaledb_fdw
 *
 * Portions Copyright (c) 2012-2018, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include <postgres.h>

#include "scan_plan.h"

#include <access/reloptions.h>
#include <catalog/pg_foreign_server.h>
#include <catalog/pg_foreign_table.h>
#include <catalog/pg_user_mapping.h>
#include <commands/defrem.h>
#include <commands/extension.h>
#include <utils/builtins.h>
#include <utils/varlena.h>
#include <libpq-fe.h>

#include <remote/connection.h>
#include "option.h"

/*
 * Describes the valid options for objects that this wrapper uses.
 */
typedef struct TsFdwOption
{
	const char *keyword;
	Oid optcontext; /* OID of catalog in which option may appear */
} TsFdwOption;

/*
 * Valid options for timescaledb_fdw.
 * Allocated and filled in init_ts_fdw_options
 */
static TsFdwOption *timescaledb_fdw_options = NULL;

/*
 * Helper functions
 */
static void init_ts_fdw_options(void);
static bool is_valid_option(const char *keyword, Oid context);

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses timescaledb_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
void
option_validate(List *options_list, Oid catalog)
{
	ListCell *cell;

	/* Build our options lists if we didn't yet. */
	init_ts_fdw_options();

	/*
	 * Check that only options supported by timescaledb_fdw, and allowed for
	 * the current object type, are given.
	 */
	foreach (cell, options_list)
	{
		DefElem *def = (DefElem *) lfirst(cell);

		if (!is_valid_option(def->defname, catalog))
		{
			/*
			 * Unknown option specified, complain about it. Provide a hint
			 * with list of valid options for the object.
			 */
			TsFdwOption *opt;
			StringInfoData buf;

			initStringInfo(&buf);
			for (opt = timescaledb_fdw_options; opt->keyword; opt++)
			{
				if (catalog == opt->optcontext)
					appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "", opt->keyword);
			}

			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\"", def->defname),
					 errhint("Valid options in this context are: %s", buf.data)));
		}

		/*
		 * Validate option value, when we can do so without any context.
		 */
		if (strcmp(def->defname, "use_remote_estimate") == 0)
		{
			/* these accept only boolean values */
			(void) defGetBoolean(def);
		}
		else if (strcmp(def->defname, "fdw_startup_cost") == 0 ||
				 strcmp(def->defname, "fdw_tuple_cost") == 0)
		{
			/* these must have a non-negative numeric value */
			double val;
			char *endp;

			val = strtod(defGetString(def), &endp);
			if (*endp || val < 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("%s requires a non-negative numeric value", def->defname)));
		}
		else if (strcmp(def->defname, "extensions") == 0)
		{
			/* check list syntax, warn about uninstalled extensions */
			(void) option_extract_extension_list(defGetString(def), true);
		}
		else if (strcmp(def->defname, "fetch_size") == 0)
		{
			int fetch_size;

			fetch_size = strtol(defGetString(def), NULL, 10);
			if (fetch_size <= 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("%s requires a non-negative integer value", def->defname)));
		}
	}
}

/*
 * Initialize option lists.
 */
static void
init_ts_fdw_options(void)
{
	/* non-libpq FDW-specific FDW options */
	static const TsFdwOption non_libpq_options[] = {
		/* use_remote_estimate is available on both server and table */
		{ "use_remote_estimate", ForeignServerRelationId },
		{ "use_remote_estimate", ForeignTableRelationId },
		/* cost factors */
		{ "fdw_startup_cost", ForeignServerRelationId },
		{ "fdw_tuple_cost", ForeignServerRelationId },
		/* shippable extensions */
		{ "extensions", ForeignServerRelationId },
		/* fetch_size is available on both server and table */
		{ "fetch_size", ForeignServerRelationId },
		{ "fetch_size", ForeignTableRelationId },
		{ NULL, InvalidOid }
	};

	/* Prevent redundant initialization. */
	if (timescaledb_fdw_options)
		return;

	/*
	 * Construct an array which consists of the FDW-specific options.
	 *
	 * We use plain malloc here to allocate timescaledb_fdw_options because it
	 * lives as long as the backend process does.
	 */
	timescaledb_fdw_options =
		(TsFdwOption *) malloc(sizeof(TsFdwOption) * sizeof(non_libpq_options));

	if (timescaledb_fdw_options == NULL)
		ereport(ERROR, (errcode(ERRCODE_FDW_OUT_OF_MEMORY), errmsg("out of memory")));

	/* Append FDW-specific options and dummy terminator. */
	memcpy(timescaledb_fdw_options, non_libpq_options, sizeof(non_libpq_options));
}

/*
 * Check whether the given option is one of the valid timescaledb_fdw options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool
is_valid_option(const char *keyword, Oid context)
{
	TsFdwOption *opt;

	Assert(timescaledb_fdw_options); /* must be initialized already */

	switch (remote_connection_option_type(keyword))
	{
		case CONN_OPTION_TYPE_SERVER:
			return true;
		case CONN_OPTION_TYPE_USER:
			return true;
		case CONN_OPTION_TYPE_NONE:
			for (opt = timescaledb_fdw_options; opt->keyword; opt++)
			{
				if (context == opt->optcontext && strcmp(opt->keyword, keyword) == 0)
					return true;
			}
	}

	return false;
}

/*
 * Parse a comma-separated string and return a List of the OIDs of the
 * extensions named in the string.  If any names in the list cannot be found,
 * report a warning if warn_on_missing is true, else just silently ignore
 * them.
 */
List *
option_extract_extension_list(const char *extensions_string, bool warn_on_missing)
{
	List *extension_oids = NIL;
	List *extlist;
	ListCell *lc;

	/* SplitIdentifierString scribbles on its input, so pstrdup first */
	if (!SplitIdentifierString(pstrdup(extensions_string), ',', &extlist))
	{
		/* syntax error in name list */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("parameter \"%s\" must be a list of extension names", "extensions")));
	}

	foreach (lc, extlist)
	{
		const char *extension_name = (const char *) lfirst(lc);
		Oid extension_oid = get_extension_oid(extension_name, true);

		if (OidIsValid(extension_oid))
			extension_oids = lappend_oid(extension_oids, extension_oid);
		else if (warn_on_missing)
			ereport(WARNING,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("extension \"%s\" is not installed", extension_name)));
	}

	list_free(extlist);

	return extension_oids;
}
