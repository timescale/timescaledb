/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <fmgr.h>

#include <access/htup_details.h>
#include <catalog/pg_type.h>
#include <commands/defrem.h>
#include <nodes/parsenodes.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>

#include "debug_assert.h"
#include "extension_constants.h"
#include "with_clause_parser.h"

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
			(pg_strcasecmp(def->defnamespace, EXTENSION_NAMESPACE) == 0 ||
			 pg_strcasecmp(def->defnamespace, EXTENSION_NAMESPACE_ALIAS) == 0))
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

static char *
ts_with_clause_definition_names(const WithClauseDefinition *args, Size nargs)
{
	StringInfoData buf;
	Size i;

	initStringInfo(&buf);

	for (i = 0; i < nargs; i++)
	{
		if (i > 0)
			appendStringInfoString(&buf, ", ");
		appendStringInfoString(&buf, args[i].arg_names[0]);
	}

	return buf.data;
}

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
		results[i].definition = &args[i];
		results[i].parsed = args[i].default_val;
		results[i].is_default = true;
	}

	foreach (cell, def_elems)
	{
		DefElem *def = (DefElem *) lfirst(cell);
		bool argument_recognized = false;

		for (i = 0; i < nargs; i++)
		{
			for (int j = 0; args[i].arg_names[j] != NULL; ++j)
			{
				if (pg_strcasecmp(def->defname, args[i].arg_names[j]) == 0)
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
		}

		if (!argument_recognized)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unrecognized parameter \"%s.%s\"", def->defnamespace, def->defname),
					 errhint("Valid timescaledb parameters are: %s",
							 ts_with_clause_definition_names(args, nargs))));
	}

	return results;
}

/*
 * This function handles parsing of WITH clauses for ALTER TABLE RESET.
 * Unlike ts_with_clauses_parse, it does not parse any option values,
 * as RESET clauses only include option names without associated values.
 */
WithClauseResult *
ts_with_clauses_parse_reset(const List *def_elems, const WithClauseDefinition *args, Size nargs)
{
	ListCell *cell;
	WithClauseResult *results = palloc0(sizeof(*results) * nargs);
	Size i;

	for (i = 0; i < nargs; i++)
	{
		results[i].definition = &args[i];
		results[i].parsed = args[i].default_val;
		results[i].is_default = true;
	}

	foreach (cell, def_elems)
	{
		DefElem *def = (DefElem *) lfirst(cell);
		bool argument_recognized = false;

		for (i = 0; i < nargs; i++)
		{
			for (int j = 0; args[i].arg_names[j] != NULL; ++j)
			{
				if (pg_strcasecmp(def->defname, args[i].arg_names[j]) == 0)
				{
					argument_recognized = true;

					if (!results[i].is_default)
						ereport(ERROR,
								(errcode(ERRCODE_AMBIGUOUS_PARAMETER),
								 errmsg("duplicate parameter \"%s.%s\"",
										def->defnamespace,
										def->defname)));

					results[i].is_default = false;
					break;
				}
			}
		}

		if (!argument_recognized)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unrecognized parameter \"%s.%s\"", def->defnamespace, def->defname),
					 errhint("Valid timescaledb parameters are: %s",
							 ts_with_clause_definition_names(args, nargs))));
	}

	return results;
}

extern TSDLLEXPORT char *
ts_with_clause_result_deparse_value(const WithClauseResult *result)
{
	Ensure(OidIsValid(result->definition->type_id),
		   "argument \"%d\" has invalid OID",
		   result->definition->type_id);

	return ts_datum_to_string(result->parsed, result->definition->type_id);
}

static Datum
parse_arg(WithClauseDefinition arg, DefElem *def)
{
	char *value;
	Datum val;
	Oid in_fn;
	Oid typIOParam;

	if (!OidIsValid(arg.type_id))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_PARAMETER),
				 errmsg("argument \"%s.%s\" not implemented", def->defnamespace, def->defname)));

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

	/*
	 * We could use InputFunctionCallSafe() here but this is just supported
	 * for PG16 and later, so we opt for checking if the failure is what we
	 * expected and re-throwing the error otherwise.
	 */
	PG_TRY();
	{
		val = OidInputFunctionCall(in_fn, value, typIOParam, -1);
	}
	PG_CATCH();
	{
		const int sqlerrcode = geterrcode();
		/*
		 * We can deal with the Data Exception category and in the Syntax
		 * Error or Access Rule Violation category, but if the error is an
		 * insufficient resources category, for example, an out of memory
		 * error, we should just re-throw it.
		 *
		 * Errors in other categories are unlikely, but we cannot do anything
		 * with them anyway, so just re-throw them as well.
		 */
		if (ERRCODE_TO_CATEGORY(sqlerrcode) != ERRCODE_DATA_EXCEPTION &&
			ERRCODE_TO_CATEGORY(sqlerrcode) != ERRCODE_SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION)
		{
			PG_RE_THROW();
		}
		FlushErrorState();

		/* We are currently using the ErrorContext, but since we are going to
		 * raise an error later, there is no reason to switch memory context
		 * nor restore the resource owner here. */

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
