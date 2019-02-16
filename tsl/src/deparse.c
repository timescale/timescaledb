/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <utils/rel.h>
#include <lib/stringinfo.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/relcache.h>
#include <catalog/indexing.h>
#include <utils/ruleutils.h>
#include <utils/syscache.h>
#include <commands/tablespace.h>
#include <catalog/pg_class.h>
#include <utils/rel.h>
#include <access/relscan.h>
#include <utils/fmgroids.h>
#include <catalog/pg_constraint.h>
#include <catalog/pg_index.h>
#include <nodes/pg_list.h>
#include "export.h"
#include "compat.h"
#include "trigger.h"
#include "utils.h"
#include "deparse.h"
#include <extension.h>
#include <utils/lsyscache.h>

/**
 * Deparse a table into a set of SQL commands that can be used to recreate it.
 * Together with column definiton it deparses constraints, indexes, triggers and rules as well.
 * There are some table types that are not supported: temporary, partitioned,
 * foreign, inherited and a table that uses options. Row security is also not supported.
 */

typedef char *(*GetCmdFunc)(Oid oid);

static char *
get_index_cmd(Oid oid)
{
	return pg_get_indexdef_string(oid);
}

static char *
get_constraint_cmd(Oid oid)
{
	return pg_get_constraintdef_command(oid);
}

static FunctionCallInfoData *
build_fcinfo_data(Oid oid)
{
	FunctionCallInfoData *fcinfo = palloc(sizeof(FunctionCallInfoData));

	InitFunctionCallInfoData(*fcinfo, NULL, 1, InvalidOid, NULL, NULL);
	fcinfo->arg[0] = Int32GetDatum(oid);
	fcinfo->argnull[0] = false;
	return fcinfo;
}

static char *
get_trigger_cmd(Oid oid)
{
	return TextDatumGetCString(pg_get_triggerdef(build_fcinfo_data(oid)));
}

static char *
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
get_rule_cmds(List *rule_oids)
{
	return get_cmds(rule_oids, get_rule_cmd);
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

		if (attr->attisdropped)
			continue;

		appendStringInfo(stmt,
						 "\"%s\" %s",
						 NameStr(attr->attname),
						 format_type_with_typemod(attr->atttypid, attr->atttypmod));

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
					char *attr_default =
						TextDatumGetCString(DirectFunctionCall2(pg_get_expr,
																CStringGetTextDatum(attr_def.adbin),
																ObjectIdGetDatum(rel->rd_id)));

					appendStringInfo(stmt, " DEFAULT %s", attr_default);
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

static bool
add_constraint(HeapTuple constraint_tuple, void *ctx)
{
	ConstraintContext *cc = ctx;
	Form_pg_constraint constraint = (Form_pg_constraint) GETSTRUCT(constraint_tuple);

	if (constraint->conindid != 0)
		*cc->constraint_indexes = lappend_oid(*cc->constraint_indexes, constraint->conindid);
	cc->constraints = lappend_oid(cc->constraints, HeapTupleGetOid(constraint_tuple));
	return true;
}

static List *
get_constraint_oids(Oid relid, List **constraint_indexes)
{
	ConstraintContext *cc = palloc(sizeof(ConstraintContext));

	cc->constraints = NIL;
	cc->constraint_indexes = constraint_indexes;
	ts_process_constraints(relid, add_constraint, cc);
	return cc->constraints;
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
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("temporary table is not supported")));
	}
	if (rel->rd_rel->relrowsecurity)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("row security is not supported")));
}

TableInfo *
deparse_create_table_info(Oid relid)
{
	List *exclude_indexes = NIL;
	TableInfo *table_info = palloc(sizeof(TableInfo));
	Relation rel = relation_open(relid, AccessShareLock);

	if (rel == NULL)
		ereport(ERROR, (errmsg("relation with id %d not found", relid)));

	validate_relation(rel);

	table_info->relid = relid;
	table_info->constraints = get_constraint_oids(relid, &exclude_indexes);
	table_info->indexes = get_index_oids(rel, exclude_indexes);
	table_info->triggers = get_trigger_oids(rel);
	table_info->rules = get_rule_oids(rel);
	relation_close(rel, AccessShareLock);
	return table_info;
}

TableDef *
deparse_get_tabledef(TableInfo *table_info)
{
	StringInfo create_table = makeStringInfo();
	StringInfo set_schema = makeStringInfo();
	TableDef *table_def = palloc(sizeof(TableDef));
	Oid tablespace;
	Relation rel = relation_open(table_info->relid, AccessShareLock);

	appendStringInfo(set_schema,
					 "SET SCHEMA '%s';",
					 quote_identifier(get_namespace_name(rel->rd_rel->relnamespace)));
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

	tablespace = get_rel_tablespace(table_info->relid);
	if (tablespace != InvalidOid)
		appendStringInfo(create_table, " TABLESPACE %s", get_tablespace_name(tablespace));

	appendStringInfoChar(create_table, ';');
	table_def->create_cmd = create_table->data;

	table_def->constraint_cmds = get_constraint_cmds(table_info->constraints);
	table_def->index_cmds = get_index_cmds(table_info->indexes);
	table_def->trigger_cmds = get_trigger_cmds(table_info->triggers);
	table_def->rule_cmds = get_rule_cmds(table_info->rules);

	relation_close(rel, AccessShareLock);
	return table_def;
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
		appendStringInfo(dim_cmd, "chunk_time_interval => %ld);", dimension->fd.interval_length);

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

	appendStringInfo(hypertable_cmd, ", chunk_time_interval => %ld", time_dim->fd.interval_length);

	if (OidIsValid(ht->chunk_sizing_func))
	{
		appendStringInfo(hypertable_cmd,
						 ", chunk_sizing_func => %s",
						 quote_literal_cstr(
							 quote_qualified_identifier(NameStr(ht->fd.chunk_sizing_func_schema),
														NameStr(ht->fd.chunk_sizing_func_name))));
		appendStringInfo(hypertable_cmd, ", chunk_target_size => '%ld'", ht->fd.chunk_target_size);
	}

	/*
	 * Backend is assumed to not have any preexisting conflicting table or hypertable.  Any default
	 * indicies will have already been created by the frontend.
	 */
	appendStringInfoString(hypertable_cmd, ", if_not_exists => FALSE");
	appendStringInfoString(hypertable_cmd, ", migrate_data => FALSE");
	appendStringInfoString(hypertable_cmd, ", create_default_indexes => FALSE");
	appendStringInfoString(hypertable_cmd, ", replication_factor => 0");

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

	return result;
}
