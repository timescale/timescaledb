/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <utils/rel.h>
#include <lib/stringinfo.h>
#include <utils/acl.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/relcache.h>
#include <utils/ruleutils.h>
#include <utils/syscache.h>
#include <utils/fmgroids.h>
#include <utils/rel.h>
#include <commands/tablespace.h>
#include <commands/defrem.h>
#include <access/relscan.h>
#include <catalog/pg_class.h>
#include <catalog/indexing.h>
#include <catalog/pg_constraint.h>
#include <catalog/pg_index.h>
#include <catalog/pg_authid.h>
#include <catalog/pg_proc.h>
#include <catalog/namespace.h>
#include <nodes/pg_list.h>
#include <funcapi.h>
#include <fmgr.h>

#include <constraint.h>
#include <extension.h>
#include <utils.h>
#include <export.h>
#include <compat/compat.h>
#include <trigger.h>

#include "deparse.h"
#include "guc.h"
#include "utils.h"

/*
 * Deparse a table into a set of SQL commands that can be used to recreate it.
 * Together with column definition it deparses constraints, indexes, triggers
 * and rules as well.  There are some table types that are not supported:
 * temporary, partitioned, foreign, inherited and a table that uses
 * options. Row security is also not supported.
 */
typedef const char *(*GetCmdFunc)(Oid oid);

static const char *
get_index_cmd(Oid oid)
{
	return pg_get_indexdef_string(oid);
}

static const char *
get_constraint_cmd(Oid oid)
{
	return pg_get_constraintdef_command(oid);
}

static FunctionCallInfo
build_fcinfo_data(Oid oid)
{
	FunctionCallInfo fcinfo = palloc(SizeForFunctionCallInfo(1));

	InitFunctionCallInfoData(*fcinfo, NULL, 1, InvalidOid, NULL, NULL);
	FC_ARG(fcinfo, 0) = ObjectIdGetDatum(oid);
	FC_NULL(fcinfo, 0) = false;

	return fcinfo;
}

static const char *
get_trigger_cmd(Oid oid)
{
	return TextDatumGetCString(pg_get_triggerdef(build_fcinfo_data(oid)));
}

static const char *
get_function_cmd(Oid oid)
{
	return TextDatumGetCString(pg_get_functiondef(build_fcinfo_data(oid)));
}

static const char *
get_rule_cmd(Oid oid)
{
	return TextDatumGetCString(pg_get_ruledef(build_fcinfo_data(oid)));
}

static List *
get_cmds(List *oids, GetCmdFunc get_cmd)
{
	List *cmds = NIL;
	ListCell *cell;

	foreach (cell, oids)
	{
		StringInfo cmd = makeStringInfo();

		appendStringInfo(cmd, "%s;", get_cmd(lfirst_oid(cell)));
		cmds = lappend(cmds, cmd->data);
	}
	return cmds;
}

static List *
get_constraint_cmds(List *constraint_oids)
{
	return get_cmds(constraint_oids, get_constraint_cmd);
}

static List *
get_index_cmds(List *index_oids)
{
	return get_cmds(index_oids, get_index_cmd);
}

static List *
get_trigger_cmds(List *trigger_oids)
{
	return get_cmds(trigger_oids, get_trigger_cmd);
}

static List *
get_function_cmds(List *function_oids)
{
	return get_cmds(function_oids, get_function_cmd);
}

static List *
get_rule_cmds(List *rule_oids)
{
	return get_cmds(rule_oids, get_rule_cmd);
}

static bool
column_is_serial(Relation rel, Name column)
{
	const char *relation_name;
	LOCAL_FCINFO(fcinfo, 2);

	/* Prepare call to pg_get_serial_sequence() function.
	 *
	 * We have to manually prepare the function call context here instead
	 * of using the DirectFunctionCall2() because we expect to get
	 * NULL return value. */
	relation_name = quote_qualified_identifier(get_namespace_name(rel->rd_rel->relnamespace),
											   NameStr(rel->rd_rel->relname));
	InitFunctionCallInfoData(*fcinfo, NULL, 2, InvalidOid, NULL, NULL);
	FC_ARG(fcinfo, 0) = CStringGetTextDatum(relation_name);
	FC_ARG(fcinfo, 1) = CStringGetTextDatum(column->data);
	FC_NULL(fcinfo, 0) = false;
	FC_NULL(fcinfo, 1) = false;
	pg_get_serial_sequence(fcinfo);

	return !fcinfo->isnull;
}

static void
deparse_columns(StringInfo stmt, Relation rel)
{
	int att_idx;
	TupleDesc rel_desc = RelationGetDescr(rel);
	TupleConstr *constraints = rel_desc->constr;

	for (att_idx = 0; att_idx < rel_desc->natts; att_idx++)
	{
		int dim_idx;
		Form_pg_attribute attr = TupleDescAttr(rel_desc, att_idx);
		bits16 flags = FORMAT_TYPE_TYPEMOD_GIVEN;

		if (attr->attisdropped)
			continue;

		/*
		 * if it's not a builtin type then schema qualify the same. There's a function
		 * deparse_type_name in fdw, but we don't want cross linking unnecessarily
		 */
		if (attr->atttypid >= FirstBootstrapObjectIdCompat)
			flags |= FORMAT_TYPE_FORCE_QUALIFY;

		appendStringInfo(stmt,
						 "\"%s\" %s",
						 NameStr(attr->attname),
						 format_type_extended(attr->atttypid, attr->atttypmod, flags));

		if (attr->attnotnull)
			appendStringInfoString(stmt, " NOT NULL");

		if (OidIsValid(attr->attcollation))
			appendStringInfo(stmt, " COLLATE \"%s\"", get_collation_name(attr->attcollation));

		if (attr->atthasdef)
		{
			int co_idx;

			for (co_idx = 0; co_idx < constraints->num_defval; co_idx++)
			{
				AttrDefault attr_def = constraints->defval[co_idx];

				if (attr->attnum == attr_def.adnum)
				{
					char *attr_default;

					/* Skip default expression in case if column is serial
					 * (has dependant sequence object) */
					if (column_is_serial(rel, &attr->attname))
						break;

					attr_default =
						TextDatumGetCString(DirectFunctionCall2(pg_get_expr,
																CStringGetTextDatum(attr_def.adbin),
																ObjectIdGetDatum(rel->rd_id)));

					if (attr->attgenerated == ATTRIBUTE_GENERATED_STORED)
						appendStringInfo(stmt, " GENERATED ALWAYS AS %s STORED", attr_default);
					else
					{
						appendStringInfo(stmt, " DEFAULT %s", attr_default);
					}
					break;
				}
			}
		}

		for (dim_idx = 1; dim_idx < attr->attndims; dim_idx++)
			appendStringInfoString(stmt, "[]");

		if (att_idx != (rel_desc->natts - 1))
			appendStringInfoString(stmt, ", ");
	}
}

typedef struct ConstraintContext
{
	List *constraints;
	List **constraint_indexes;
} ConstraintContext;

static ConstraintProcessStatus
add_constraint(HeapTuple constraint_tuple, void *ctx)
{
	ConstraintContext *cc = ctx;
	Form_pg_constraint constraint = (Form_pg_constraint) GETSTRUCT(constraint_tuple);
	Oid constroid;

	if (OidIsValid(constraint->conindid))
		*cc->constraint_indexes = lappend_oid(*cc->constraint_indexes, constraint->conindid);
	constroid = constraint->oid;
	cc->constraints = lappend_oid(cc->constraints, constroid);
	return CONSTR_PROCESSED;
}

static List *
get_constraint_oids(Oid relid, List **constraint_indexes)
{
	ConstraintContext cc = {
		.constraints = NIL,
		.constraint_indexes = constraint_indexes,
	};

	ts_constraint_process(relid, add_constraint, &cc);

	return cc.constraints;
}

static List *
get_index_oids(Relation rel, List *exclude_indexes)
{
	List *indexes = NIL;
	ListCell *cell;

	foreach (cell, RelationGetIndexList(rel))
	{
		Oid indexid = lfirst_oid(cell);

		if (!list_member_oid(exclude_indexes, indexid))
			indexes = lappend_oid(indexes, indexid);
	}
	return indexes;
}

/*
 *  Specifically exclude the hypertable insert blocker from this list.  A table which was recreated
 * with that trigger present would not be able to made into a hypertable.
 */
static List *
get_trigger_oids(Relation rel)
{
	List *triggers = NIL;

	if (rel->trigdesc != NULL)
	{
		int i;

		for (i = 0; i < rel->trigdesc->numtriggers; i++)
		{
			const Trigger trigger = rel->trigdesc->triggers[i];

			if (!trigger.tgisinternal && strcmp(trigger.tgname, INSERT_BLOCKER_NAME) != 0)
				triggers = lappend_oid(triggers, trigger.tgoid);
		}
	}
	return triggers;
}

static List *
get_trigger_function_oids(Relation rel)
{
	List *functions = NIL;

	if (rel->trigdesc != NULL)
	{
		int i;

		for (i = 0; i < rel->trigdesc->numtriggers; i++)
		{
			const Trigger trigger = rel->trigdesc->triggers[i];

			if (!trigger.tgisinternal && strcmp(trigger.tgname, INSERT_BLOCKER_NAME) != 0)
				functions = lappend_oid(functions, trigger.tgfoid);
		}
	}

	return functions;
}

static List *
get_rule_oids(Relation rel)
{
	List *rules = NIL;

	if (rel->rd_rules != NULL)
	{
		int i;

		for (i = 0; i < rel->rd_rules->numLocks; i++)
		{
			const RewriteRule *rule = rel->rd_rules->rules[i];

			rules = lappend_oid(rules, rule->ruleId);
		}
	}
	return rules;
}

static void
validate_relation(Relation rel)
{
	if (rel->rd_rel->relkind != RELKIND_RELATION)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("given relation is not an ordinary table")));

	if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("temporary table is not supported")));

	if (rel->rd_rel->relrowsecurity)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("row security is not supported")));
}

TableInfo *
deparse_create_table_info(Oid relid)
{
	List *exclude_indexes = NIL;
	TableInfo *table_info = palloc0(sizeof(TableInfo));
	Relation rel = table_open(relid, AccessShareLock);

	if (rel == NULL)
		ereport(ERROR, (errmsg("relation with id %u not found", relid)));

	validate_relation(rel);

	table_info->relid = relid;
	table_info->constraints = get_constraint_oids(relid, &exclude_indexes);
	table_info->indexes = get_index_oids(rel, exclude_indexes);
	table_info->triggers = get_trigger_oids(rel);
	table_info->functions = get_trigger_function_oids(rel);
	table_info->rules = get_rule_oids(rel);
	table_close(rel, AccessShareLock);
	return table_info;
}

static void
deparse_get_tabledef_with(const TableInfo *table_info, StringInfo create_table)
{
	ListCell *cell;
	List *opts = ts_get_reloptions(table_info->relid);

	if (list_length(opts) == 0)
		return;

	appendStringInfoString(create_table, " WITH (");

	foreach (cell, opts)
	{
		DefElem *def = (DefElem *) lfirst(cell);

		appendStringInfo(create_table,
						 "%s%s=%s",
						 cell != list_head(opts) ? "," : "",
						 def->defname,
						 defGetString(def));
	}

	appendStringInfoChar(create_table, ')');
}

TableDef *
deparse_get_tabledef(TableInfo *table_info)
{
	StringInfo create_table = makeStringInfo();
	StringInfo set_schema = makeStringInfo();
	TableDef *table_def = palloc0(sizeof(TableDef));
	Relation rel = table_open(table_info->relid, AccessShareLock);

	appendStringInfo(set_schema,
					 "SET SCHEMA %s;",
					 quote_literal_cstr(get_namespace_name(rel->rd_rel->relnamespace)));
	table_def->schema_cmd = set_schema->data;

	appendStringInfoString(create_table, "CREATE");
	if (rel->rd_rel->relpersistence == RELPERSISTENCE_UNLOGGED)
		appendStringInfoString(create_table, " UNLOGGED");
	appendStringInfoString(create_table, " TABLE");

	appendStringInfo(create_table,
					 " \"%s\".\"%s\" (",
					 get_namespace_name(rel->rd_rel->relnamespace),
					 NameStr(rel->rd_rel->relname));

	deparse_columns(create_table, rel);

	appendStringInfoChar(create_table, ')');
	appendStringInfo(create_table, " USING \"%s\" ", get_am_name(rel->rd_rel->relam));
	deparse_get_tabledef_with(table_info, create_table);

	appendStringInfoChar(create_table, ';');
	table_def->create_cmd = create_table->data;

	table_def->constraint_cmds = get_constraint_cmds(table_info->constraints);
	table_def->index_cmds = get_index_cmds(table_info->indexes);
	table_def->trigger_cmds = get_trigger_cmds(table_info->triggers);
	table_def->function_cmds = get_function_cmds(table_info->functions);
	table_def->rule_cmds = get_rule_cmds(table_info->rules);

	table_close(rel, AccessShareLock);
	return table_def;
}

/*
 * Append a privilege name to a string if the privilege is set.
 *
 * Parameters:
 *    buf: Buffer to append to.
 *    pfirst: Pointer to variable to remember if elements are already added.
 *    privs: Bitmap of privilege flags.
 *    mask: Mask for privilege to check.
 *    priv_name: String with name of privilege to add.
 */
static void
append_priv_if_set(StringInfo buf, bool *priv_added, uint32 privs, uint32 mask,
				   const char *priv_name)
{
	if (privs & mask)
	{
		if (*priv_added)
			appendStringInfoString(buf, ", ");
		else
			*priv_added = true;
		appendStringInfoString(buf, priv_name);
	}
}

static void
append_privs_as_text(StringInfo buf, uint32 privs)
{
	bool priv_added = false;
	append_priv_if_set(buf, &priv_added, privs, ACL_INSERT, "INSERT");
	append_priv_if_set(buf, &priv_added, privs, ACL_SELECT, "SELECT");
	append_priv_if_set(buf, &priv_added, privs, ACL_UPDATE, "UPDATE");
	append_priv_if_set(buf, &priv_added, privs, ACL_DELETE, "DELETE");
	append_priv_if_set(buf, &priv_added, privs, ACL_TRUNCATE, "TRUNCATE");
	append_priv_if_set(buf, &priv_added, privs, ACL_REFERENCES, "REFERENCES");
	append_priv_if_set(buf, &priv_added, privs, ACL_TRIGGER, "TRIGGER");
}

/*
 * Create grant statements for a relation.
 *
 * This will create a list of grant statements, one for each role.
 */
static List *
deparse_grant_commands_for_relid(Oid relid)
{
	HeapTuple reltup;
	Form_pg_class pg_class_tuple;
	List *cmds = NIL;
	Datum acl_datum;
	bool is_null;
	Oid owner_id;
	Acl *acl;
	int i;
	const AclItem *acldat;

	reltup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(reltup))
		elog(ERROR, "cache lookup failed for relation %u", relid);
	pg_class_tuple = (Form_pg_class) GETSTRUCT(reltup);

	if (pg_class_tuple->relkind != RELKIND_RELATION)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("\"%s\" is not an ordinary table", NameStr(pg_class_tuple->relname))));

	owner_id = pg_class_tuple->relowner;
	acl_datum = SysCacheGetAttr(RELOID, reltup, Anum_pg_class_relacl, &is_null);

	if (is_null)
		acl = acldefault(OBJECT_TABLE, owner_id);
	else
		acl = DatumGetAclP(acl_datum);

	acldat = ACL_DAT(acl);
	for (i = 0; i < ACL_NUM(acl); i++)
	{
		const AclItem *aclitem = &acldat[i];
		Oid role_id = aclitem->ai_grantee;
		StringInfo grant_cmd;
		HeapTuple utup;

		/* We skip the owner of the table since she automatically have all
		 * privileges on the table. */
		if (role_id == owner_id)
			continue;

		grant_cmd = makeStringInfo();
		utup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(role_id));

		if (!HeapTupleIsValid(utup))
			continue;

		appendStringInfoString(grant_cmd, "GRANT ");
		append_privs_as_text(grant_cmd, aclitem->ai_privs);
		appendStringInfo(grant_cmd,
						 " ON TABLE %s.%s TO %s",
						 quote_identifier(get_namespace_name(pg_class_tuple->relnamespace)),
						 quote_identifier(NameStr(pg_class_tuple->relname)),
						 quote_identifier(NameStr(((Form_pg_authid) GETSTRUCT(utup))->rolname)));

		ReleaseSysCache(utup);
		cmds = lappend(cmds, grant_cmd->data);
	}

	ReleaseSysCache(reltup);

	return cmds;
}

List *
deparse_get_tabledef_commands(Oid relid)
{
	TableInfo *table_info = deparse_create_table_info(relid);
	TableDef *table_def = deparse_get_tabledef(table_info);

	return deparse_get_tabledef_commands_from_tabledef(table_def);
}

List *
deparse_get_tabledef_commands_from_tabledef(TableDef *table_def)
{
	List *cmds = NIL;

	cmds = lappend(cmds, (char *) table_def->schema_cmd);
	cmds = lappend(cmds, (char *) table_def->create_cmd);
	cmds = list_concat(cmds, table_def->constraint_cmds);
	cmds = list_concat(cmds, table_def->index_cmds);
	cmds = list_concat(cmds, table_def->function_cmds);
	cmds = list_concat(cmds, table_def->trigger_cmds);
	cmds = list_concat(cmds, table_def->rule_cmds);
	return cmds;
}

const char *
deparse_get_tabledef_commands_concat(Oid relid)
{
	StringInfo tabledef = makeStringInfo();
	ListCell *cell;

	foreach (cell, deparse_get_tabledef_commands(relid))
		appendStringInfoString(tabledef, lfirst(cell));

	return tabledef->data;
}

static const char *
deparse_get_add_dimension_command(Hypertable *ht, Dimension *dimension)
{
	StringInfo dim_cmd = makeStringInfo();

	appendStringInfo(dim_cmd,
					 "SELECT * FROM %s.add_dimension(%s, %s, ",
					 quote_identifier(ts_extension_schema_name()),
					 quote_literal_cstr(
						 quote_qualified_identifier(get_namespace_name(
														get_rel_namespace(ht->main_table_relid)),
													get_rel_name(ht->main_table_relid))),
					 quote_literal_cstr(NameStr(dimension->fd.column_name)));

	if (dimension->type == DIMENSION_TYPE_CLOSED)
		appendStringInfo(dim_cmd,
						 "number_partitions => %d, partitioning_func => %s);",
						 dimension->fd.num_slices,
						 quote_literal_cstr(
							 quote_qualified_identifier(NameStr(
															dimension->fd.partitioning_func_schema),
														NameStr(dimension->fd.partitioning_func))));
	else
		appendStringInfo(dim_cmd,
						 "chunk_time_interval => " INT64_FORMAT ");",
						 dimension->fd.interval_length);

	return dim_cmd->data;
}

DeparsedHypertableCommands *
deparse_get_distributed_hypertable_create_command(Hypertable *ht)
{
	Hyperspace *space = ht->space;
	Dimension *time_dim = &space->dimensions[0];
	StringInfo hypertable_cmd = makeStringInfo();
	DeparsedHypertableCommands *result = palloc(sizeof(DeparsedHypertableCommands));

	appendStringInfo(hypertable_cmd,
					 "SELECT * FROM %s.create_hypertable(%s",
					 quote_identifier(ts_extension_schema_name()),
					 quote_literal_cstr(
						 quote_qualified_identifier(get_namespace_name(
														get_rel_namespace(ht->main_table_relid)),
													get_rel_name(ht->main_table_relid))));

	appendStringInfo(hypertable_cmd,
					 ", time_column_name => %s",
					 quote_literal_cstr(NameStr(time_dim->fd.column_name)));

	if (time_dim->fd.partitioning_func.data[0] != '\0')
		appendStringInfo(hypertable_cmd,
						 ", time_partitioning_func => %s",
						 quote_literal_cstr(
							 quote_qualified_identifier(NameStr(
															time_dim->fd.partitioning_func_schema),
														NameStr(time_dim->fd.partitioning_func))));

	appendStringInfo(hypertable_cmd,
					 ", associated_schema_name => %s",
					 quote_literal_cstr(NameStr(ht->fd.associated_schema_name)));
	appendStringInfo(hypertable_cmd,
					 ", associated_table_prefix => %s",
					 quote_literal_cstr(NameStr(ht->fd.associated_table_prefix)));

	appendStringInfo(hypertable_cmd,
					 ", chunk_time_interval => " INT64_FORMAT "",
					 time_dim->fd.interval_length);

	if (OidIsValid(ht->chunk_sizing_func))
	{
		appendStringInfo(hypertable_cmd,
						 ", chunk_sizing_func => %s",
						 quote_literal_cstr(
							 quote_qualified_identifier(NameStr(ht->fd.chunk_sizing_func_schema),
														NameStr(ht->fd.chunk_sizing_func_name))));
		appendStringInfo(hypertable_cmd,
						 ", chunk_target_size => '" INT64_FORMAT "'",
						 ht->fd.chunk_target_size);
	}

	/*
	 * Data node is assumed to not have any preexisting conflicting table or hypertable.
	 * Any default indices will have already been created by the access node.
	 */
	appendStringInfoString(hypertable_cmd, ", if_not_exists => FALSE");
	appendStringInfoString(hypertable_cmd, ", migrate_data => FALSE");
	appendStringInfoString(hypertable_cmd, ", create_default_indexes => FALSE");
	appendStringInfo(hypertable_cmd, ", replication_factor => %d", HYPERTABLE_DISTRIBUTED_MEMBER);

	appendStringInfoString(hypertable_cmd, ");");

	result->table_create_command = hypertable_cmd->data;
	result->dimension_add_commands = NIL;

	if (space->num_dimensions > 1)
	{
		int i;

		for (i = 1; i < space->num_dimensions; i++)
			result->dimension_add_commands =
				lappend(result->dimension_add_commands,
						(char *) deparse_get_add_dimension_command(ht, &space->dimensions[i]));
	}

	result->grant_commands = deparse_grant_commands_for_relid(ht->main_table_relid);

	return result;
}

#define DEFAULT_SCALAR_RESULT_NAME "*"

static void
deparse_result_type(StringInfo sql, FunctionCallInfo fcinfo)
{
	TupleDesc tupdesc;
	char *scalarname;
	Oid resulttypeid;
	int i;

	switch (get_call_result_type(fcinfo, &resulttypeid, &tupdesc))
	{
		case TYPEFUNC_SCALAR:
			/* scalar result type */
			Assert(NULL == tupdesc);
			Assert(OidIsValid(resulttypeid));

			/* Check if the function has a named OUT parameter */
			scalarname = get_func_result_name(fcinfo->flinfo->fn_oid);

			/* If there is no named OUT parameter, use the default name */
			if (NULL != scalarname)
			{
				appendStringInfoString(sql, scalarname);
				pfree(scalarname);
			}
			else
				appendStringInfoString(sql, DEFAULT_SCALAR_RESULT_NAME);
			break;
		case TYPEFUNC_COMPOSITE:
			/* determinable rowtype result */
			Assert(NULL != tupdesc);

			for (i = 0; i < tupdesc->natts; i++)
			{
				if (!tupdesc->attrs[i].attisdropped)
				{
					appendStringInfoString(sql, NameStr(tupdesc->attrs[i].attname));

					if (i < (tupdesc->natts - 1))
						appendStringInfoChar(sql, ',');
				}
			}
			break;
		case TYPEFUNC_RECORD:
			/* indeterminate rowtype result */
		case TYPEFUNC_COMPOSITE_DOMAIN:
			/* domain over determinable rowtype result */
		case TYPEFUNC_OTHER:
			elog(ERROR, "unsupported result type for deparsing");
			break;
	}
}

/*
 * Deparse a function call.
 *
 * Turn a function call back into a string. In theory, we could just call
 * deparse_expression() (ruleutils.c) on the original function expression (as
 * given by fcinfo->flinfo->fn_expr), but we'd like to support deparsing also
 * when the expression is not available (e.g., when invoking by OID from C
 * code). Further, deparse_expression() doesn't explicitly give the parameter
 * names, which is important in order to maintain forward-compatibility with
 * the remote version of the function in case it has reordered the parameters.
 */
const char *
deparse_func_call(FunctionCallInfo fcinfo)
{
	HeapTuple ftup;
	Form_pg_proc procform;
	StringInfoData sql;
	const char *funcnamespace;
	OverrideSearchPath search_path = {
		.schemas = NIL,
		.addCatalog = false,
		.addTemp = false,
	};
	Oid funcid = fcinfo->flinfo->fn_oid;
	Oid *argtypes;
	char **argnames;
	char *argmodes;
	int i;

	initStringInfo(&sql);
	appendStringInfoString(&sql, "SELECT ");
	deparse_result_type(&sql, fcinfo);

	/* First fetch the function's pg_proc row to inspect its rettype */
	ftup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));

	if (!HeapTupleIsValid(ftup))
		elog(ERROR, "cache lookup failed for function %u", funcid);

	procform = (Form_pg_proc) GETSTRUCT(ftup);
	funcnamespace = get_namespace_name(procform->pronamespace);

	/* If the function has named OUT-only parameters, then number of arguments
	 * returned by this get_func_arg_info can be greater than
	 * fcinfo->nargs. But we don't care about OUT-only arguements here. */
	get_func_arg_info(ftup, &argtypes, &argnames, &argmodes);

	appendStringInfo(&sql,
					 " FROM %s(",
					 quote_qualified_identifier(funcnamespace, NameStr(procform->proname)));
	ReleaseSysCache(ftup);

	/* Temporarily set a NULL search path. This makes identifier types (e.g.,
	 * regclass / tables) be fully qualified, which is needed since the search
	 * path on a remote node is not guaranteed to be the same. */
	PushOverrideSearchPath(&search_path);

	for (i = 0; i < fcinfo->nargs; i++)
	{
		const char *argvalstr = "NULL";
		bool add_type_cast = false;

		switch (argtypes[i])
		{
			case ANYOID:
			case ANYELEMENTOID:
				/* For pseudo types, try to resolve the "real" argument type
				 * from the function expression, if present */
				if (NULL != fcinfo->flinfo && NULL != fcinfo->flinfo->fn_expr)
				{
					Oid expr_argtype = get_fn_expr_argtype(fcinfo->flinfo, i);

					/* Function parameters that aren't typed need type casts,
					 * but only add a cast if the expr contained a "real" type
					 * and not an unknown or pseudo type. */
					if (OidIsValid(expr_argtype) && expr_argtype != UNKNOWNOID &&
						expr_argtype != argtypes[i])
						add_type_cast = true;

					argtypes[i] = expr_argtype;
				}
				break;
			default:
				break;
		}

		if (!FC_NULL(fcinfo, i))
		{
			bool isvarlena;
			Oid outfuncid;

			if (!OidIsValid(argtypes[i]))
				elog(ERROR, "invalid type for argument %d", i);

			getTypeOutputInfo(argtypes[i], &outfuncid, &isvarlena);
			Assert(OidIsValid(outfuncid));
			argvalstr = quote_literal_cstr(OidOutputFunctionCall(outfuncid, FC_ARG(fcinfo, i)));
		}

		appendStringInfo(&sql, "%s => %s", argnames[i], argvalstr);

		if (add_type_cast)
			appendStringInfo(&sql, "::%s", format_type_be(argtypes[i]));

		if (i < (fcinfo->nargs - 1))
			appendStringInfoChar(&sql, ',');
	}

	PopOverrideSearchPath();

	if (NULL != argtypes)
		pfree(argtypes);

	if (NULL != argnames)
		pfree(argnames);

	if (NULL != argmodes)
		pfree(argmodes);

	appendStringInfoChar(&sql, ')');

	return sql.data;
}

/*
 * Deparse a function by OID.
 *
 * The function arguments should be given as datums in the vararg list and
 * need to be specified in the order given by the (OID) function's signature.
 */
const char *
deparse_oid_function_call_coll(Oid funcid, Oid collation, unsigned int num_args, ...)
{
	FunctionCallInfo fcinfo = palloc(SizeForFunctionCallInfo(num_args));
	FmgrInfo flinfo;
	const char *result;
	va_list args;
	unsigned int i;

	fmgr_info(funcid, &flinfo);
	InitFunctionCallInfoData(*fcinfo, &flinfo, num_args, collation, NULL, NULL);
	va_start(args, num_args);

	for (i = 0; i < num_args; i++)
	{
		FC_ARG(fcinfo, i) = va_arg(args, Datum);
		FC_NULL(fcinfo, i) = false;
	}

	va_end(args);

	result = deparse_func_call(fcinfo);

	/* Check for null result, since caller is clearly not expecting one */
	if (fcinfo->isnull)
		elog(ERROR, "function %u returned NULL", flinfo.fn_oid);

	return result;
}

const char *
deparse_grant_revoke_on_database(const GrantStmt *stmt, const char *dbname)
{
	ListCell *lc;

	/*
	   GRANT { { CREATE | CONNECT | TEMPORARY | TEMP } [, ...] | ALL [ PRIVILEGES ] }
	   ON DATABASE database_name [, ...]
	   TO role_specification [, ...] [ WITH GRANT OPTION ]
	   [ GRANTED BY role_specification ]

	   REVOKE [ GRANT OPTION FOR ]
	   { { CREATE | CONNECT | TEMPORARY | TEMP } [, ...] | ALL [ PRIVILEGES ] }
	   ON DATABASE database_name [, ...]
	   FROM role_specification [, ...]
	   [ GRANTED BY role_specification ]
	   [ CASCADE | RESTRICT ]
	*/
	StringInfo command = makeStringInfo();

	/* GRANT/REVOKE */
	if (stmt->is_grant)
		appendStringInfoString(command, "GRANT ");
	else
		appendStringInfoString(command, "REVOKE ");

	/* privileges [, ...] | ALL */
	if (stmt->privileges == NULL)
	{
		/* ALL */
		appendStringInfoString(command, "ALL ");
	}
	else
	{
		foreach (lc, stmt->privileges)
		{
			AccessPriv *priv = lfirst(lc);

			appendStringInfo(command,
							 "%s%s ",
							 priv->priv_name,
							 lnext_compat(stmt->privileges, lc) != NULL ? "," : "");
		}
	}

	/* Set database name of the data node */
	appendStringInfo(command, "ON DATABASE %s ", quote_identifier(dbname));

	/* TO/FROM role_spec [, ...] */
	if (stmt->is_grant)
		appendStringInfoString(command, "TO ");
	else
		appendStringInfoString(command, "FROM ");

	foreach (lc, stmt->grantees)
	{
		RoleSpec *role_spec = lfirst(lc);
		const char *role_name = NULL;
		switch (role_spec->roletype)
		{
			case ROLESPEC_CSTRING:
				role_name = role_spec->rolename;
				break;
			case ROLESPEC_PUBLIC:
				role_name = "PUBLIC";
				break;
			case ROLESPEC_SESSION_USER:
				role_name = "SESSION_USER";
				break;
			case ROLESPEC_CURRENT_USER:
				role_name = "CURRENT_USER";
				break;
#if PG14_GE
			case ROLESPEC_CURRENT_ROLE:
				role_name = "CURRENT_ROLE";
				break;
#endif
		}
		appendStringInfo(command,
						 "%s%s ",
						 role_name,
						 lnext_compat(stmt->grantees, lc) != NULL ? "," : "");
	}

	if (stmt->grant_option)
		appendStringInfoString(command, "WITH GRANT OPTION ");

#if PG14_GE
	/* [ GRANTED BY role_specification ] */
	if (stmt->grantor)
		appendStringInfo(command, "GRANTED BY %s ", quote_identifier(stmt->grantor->rolename));
#endif

	/* CASCADE | RESTRICT */
	if (!stmt->is_grant && stmt->behavior == DROP_CASCADE)
		appendStringInfoString(command, "CASCADE");

	return command->data;
}

/* Deparse user-defined trigger */
const char *
deparse_create_trigger(CreateTrigStmt *stmt)
{
	ListCell *lc;
	bool found_event = false;
	bool found_first_arg = false;

	/*
	CREATE [ CONSTRAINT ] TRIGGER name { BEFORE | AFTER | INSTEAD OF } { event [ OR ... ] }
		ON table_name
		[ FROM referenced_table_name ]
		[ NOT DEFERRABLE | [ DEFERRABLE ] [ INITIALLY IMMEDIATE | INITIALLY DEFERRED ] ]
		[ REFERENCING { { OLD | NEW } TABLE [ AS ] transition_relation_name } [ ... ] ]
		[ FOR [ EACH ] { ROW | STATEMENT } ]
		[ WHEN ( condition ) ]
		EXECUTE { FUNCTION | PROCEDURE } function_name ( arguments )
	*/
	if (stmt->isconstraint)
		elog(ERROR, "deparsing constraint triggers is not supported");

	StringInfo command = makeStringInfo();
	appendStringInfo(command, "CREATE TRIGGER %s ", quote_identifier(stmt->trigname));

	if (TRIGGER_FOR_BEFORE(stmt->timing))
		appendStringInfoString(command, "BEFORE");
	else if (TRIGGER_FOR_AFTER(stmt->timing))
		appendStringInfoString(command, "AFTER");
	else if (TRIGGER_FOR_INSTEAD(stmt->timing))
		appendStringInfoString(command, "INSTEAD OF");
	else
		elog(ERROR, "unexpected timing value: %d", stmt->timing);

	if (TRIGGER_FOR_INSERT(stmt->events))
	{
		appendStringInfoString(command, " INSERT");
		found_event = true;
	}
	if (TRIGGER_FOR_DELETE(stmt->events))
	{
		if (found_event)
			appendStringInfoString(command, " OR");
		appendStringInfoString(command, " DELETE");
		found_event = true;
	}
	if (TRIGGER_FOR_UPDATE(stmt->events))
	{
		if (found_event)
			appendStringInfoString(command, " OR");
		appendStringInfoString(command, " UPDATE");
		found_event = true;
	}
	if (TRIGGER_FOR_TRUNCATE(stmt->events))
	{
		if (found_event)
			appendStringInfoString(command, " OR");
		appendStringInfoString(command, " TRUNCATE");
	}
	appendStringInfo(command,
					 " ON %s.%s",
					 quote_identifier(stmt->relation->schemaname),
					 quote_identifier(stmt->relation->relname));

	if (stmt->row)
		appendStringInfoString(command, " FOR EACH ROW");
	else
		appendStringInfoString(command, " FOR EACH STATEMENT");

	if (stmt->whenClause)
		elog(ERROR, "deparsing trigger WHEN clause is not supported");

	appendStringInfo(command, " EXECUTE FUNCTION %s(", NameListToQuotedString(stmt->funcname));
	foreach (lc, stmt->args)
	{
		if (found_first_arg)
			appendStringInfoString(command, ", ");
		else
			found_first_arg = true;
		appendStringInfoString(command, strVal(lfirst(lc)));
	}
	appendStringInfoString(command, ")");

	return command->data;
}
