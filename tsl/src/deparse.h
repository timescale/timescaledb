/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_DEPARSE_H
#define TIMESCALEDB_DEPARSE_H

#include <postgres.h>
#include <nodes/pg_list.h>

typedef struct TableInfo
{
	Oid relid;
	List *constraints;
	List *indexes;
	List *triggers;
	List *rules;
} TableInfo;

typedef struct TableDef
{
	const char *schema_cmd;
	const char *create_cmd;
	List *constraint_cmds;
	List *index_cmds;
	List *trigger_cmds;
	List *rule_cmds;
} TableDef;

typedef struct DeparsedHypertableCommands
{
	const char *table_create_command;
	List *dimension_add_commands;
} DeparsedHypertableCommands;

typedef struct Hypertable Hypertable;

TableInfo *deparse_create_table_info(Oid relid);
TableDef *deparse_get_tabledef(TableInfo *table_info);
List *deparse_get_tabledef_commands(Oid relid);
List *deparse_get_tabledef_commands_from_tabledef(TableDef *table_def);
const char *deparse_get_tabledef_commands_concat(Oid relid);

DeparsedHypertableCommands *deparse_get_distributed_hypertable_create_command(Hypertable *ht);

#endif
