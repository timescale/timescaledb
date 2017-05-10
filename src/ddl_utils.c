#include <postgres.h>
#include <tcop/deparse_utility.h>

enum ddl_cmd_type
{
	DDL_DROP_COLUMN, DDL_ADD_COLUMN, DDL_OTHER
};

static enum ddl_cmd_type
ddl_type_from_cmd(CollectedCommand *cmd)
{
	ListCell   *cell;

	if (cmd->type == SCT_AlterTable)
	{
		foreach(cell, cmd->d.alterTable.subcmds)
		{
			CollectedATSubcmd *sub = lfirst(cell);
			AlterTableCmd *subcmd = (AlterTableCmd *) sub->parsetree;

			Assert(IsA(subcmd, AlterTableCmd));

			switch (subcmd->subtype)
			{
				case AT_AddColumn:
				case AT_AddColumnRecurse:
					return DDL_ADD_COLUMN;
				case AT_DropColumn:
				case AT_DropColumnRecurse:
					return DDL_DROP_COLUMN;
				default:
					break;
			}
		}
	}

	return DDL_OTHER;
}

/*
 * Test if ddl command is an alter table add column
 */
PG_FUNCTION_INFO_V1(ddl_is_add_column);
Datum
ddl_is_add_column(PG_FUNCTION_ARGS)
{
	bool		is_add_column = DDL_ADD_COLUMN == ddl_type_from_cmd((CollectedCommand *) PG_GETARG_POINTER(0));

	PG_RETURN_BOOL(is_add_column);
}

/*
 * Test if ddl command is an alter table drop column
 */
PG_FUNCTION_INFO_V1(ddl_is_drop_column);
Datum
ddl_is_drop_column(PG_FUNCTION_ARGS)
{
	bool		is_drop_column = DDL_DROP_COLUMN == ddl_type_from_cmd((CollectedCommand *) PG_GETARG_POINTER(0));

	PG_RETURN_BOOL(is_drop_column);
}
