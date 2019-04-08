/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <c.h>
#include <fmgr.h>

#include <access/htup_details.h>
#include <catalog/pg_type.h>
#include <commands/defrem.h>
#include <nodes/parsenodes.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>

#include "with_clause_parser.h"

#define TIMESCALEDB_NAMESPACE "timescaledb"

/*
 * Filter a list of DefElem based on a namespace.
 * This function will iterate through DefElem and output up to two lists:
 *         within_namespace: every element within the namespace
 *     not_within_namespace: all the other elements
 *
 * That is, given a with clause like:
 *     WITH (foo.foo_para, bar.bar_param, baz.baz_param)
 *
 * ts_with_clause_filter(elems, "foo", in, not_in) will have
 *        in = foo.foo_para
 *    not_in = bar.bar_param, baz.baz_param
 */
void
ts_with_clause_filter(const List *def_elems, List **within_namespace, List **not_within_namespace)
{
	ListCell *cell;

	foreach (cell, def_elems)
	{
		DefElem *def = (DefElem *) lfirst(cell);

		if (def->defnamespace != NULL &&
			pg_strcasecmp(def->defnamespace, TIMESCALEDB_NAMESPACE) == 0)
		{
			if (within_namespace != NULL)
				*within_namespace = lappend(*within_namespace, def);
		}
		else if (not_within_namespace != NULL)
		{
			*not_within_namespace = lappend(*not_within_namespace, def);
		}
	}
}

static Datum parse_arg(WithClauseDefinition arg, DefElem *def);

/*
 * Deserialize and apply the values in a WITH clause based on the on_arg table.
 *
 * This function will go through every element in def_elems and search for a
 * corresponding argument in args, if one is found it will attempt to deserialize
 * the argument, using that table elements deserialize function, then apply it
 * to state.
 *
 * This is used to turn the list into a form more useful for our internal
 * functions
 */
WithClauseResult *
ts_with_clauses_parse(const List *def_elems, const WithClauseDefinition *args, Size nargs)
{
	ListCell *cell;
	WithClauseResult *results = palloc0(sizeof(*results) * nargs);
	Size i;

	for (i = 0; i < nargs; i++)
	{
		results[i].parsed = args[i].default_val;
		results[i].is_default = true;
	}

	foreach (cell, def_elems)
	{
		DefElem *def = (DefElem *) lfirst(cell);
		bool argument_recognized = false;

		for (i = 0; i < nargs; i++)
		{
			if (pg_strcasecmp(def->defname, args[i].arg_name) == 0)
			{
				argument_recognized = true;

				if (!results[i].is_default)
					ereport(ERROR,
							(errcode(ERRCODE_AMBIGUOUS_PARAMETER),
							 errmsg("duplicate parameter \"%s.%s\"",
									def->defnamespace,
									def->defname)));

				results[i].parsed = parse_arg(args[i], def);
				results[i].is_default = false;
				break;
			}
		}

		if (!argument_recognized)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unrecognized parameter \"%s.%s\"", def->defnamespace, def->defname)));
	}

	return results;
}

static Datum
parse_arg(WithClauseDefinition arg, DefElem *def)
{
	char *value;
	Datum val;
	Oid in_fn;
	Oid typIOParam;

	if (!OidIsValid(arg.type_id))
		elog(ERROR, "argument \"%s.%s\" not implemented", def->defnamespace, def->defname);

	if (def->arg != NULL)
		value = defGetString(def);
	else if (arg.type_id == BOOLOID)
		/* for booleans, postgres defines the option timescale.foo to be the same as
		 * timescaledb.foo='true' so if no value is found set it to "true" here */
		value = "true";
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("parameter \"%s.%s\" must have a value", def->defnamespace, def->defname)));

	getTypeInputInfo(arg.type_id, &in_fn, &typIOParam);

	Assert(OidIsValid(in_fn));

	PG_TRY();
	{
		val = OidInputFunctionCall(in_fn, value, typIOParam, -1);
	}
	PG_CATCH();
	{
		Form_pg_type typetup;
		HeapTuple tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(arg.type_id));
		if (!HeapTupleIsValid(tup))
			elog(ERROR,
				 "cache lookup failed for type of %s.%s '%u'",
				 def->defnamespace,
				 def->defname,
				 arg.type_id);

		typetup = (Form_pg_type) GETSTRUCT(tup);

		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid value for %s.%s '%s'", def->defnamespace, def->defname, value),
				 errhint("%s.%s must be a valid %s",
						 def->defnamespace,
						 def->defname,
						 NameStr(typetup->typname))));
	}
	PG_END_TRY();
	return val;
}
