#include <postgres.h>
#include <tcop/deparse_utility.h>

#include "ddl_utils.h"

enum ddl_cmd_type
{
	DDL_CHANGE_OWNER, DDL_OTHER
};

static enum ddl_cmd_type
ddl_alter_table_subcmd(CollectedCommand *cmd)
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
				case AT_ChangeOwner:
					return DDL_CHANGE_OWNER;
				default:
					break;
			}
		}
	}
	return DDL_OTHER;
}


Datum
ddl_change_owner_to(PG_FUNCTION_ARGS)
{
	CollectedATSubcmd *sub;
	AlterTableCmd *altersub;
	RoleSpec   *role;
	Name		user = palloc0(NAMEDATALEN);
	CollectedCommand *cmd = (CollectedCommand *) PG_GETARG_POINTER(0);

	Assert(cmd->type == SCT_AlterTable);
	Assert(list_length(cmd->d.alterTable.subcmds) == 1);
	sub = linitial(cmd->d.alterTable.subcmds);

	Assert(IsA(sub->parsetree, AlterTableCmd));
	altersub = (AlterTableCmd *) sub->parsetree;

	Assert(IsA(altersub->newowner, RoleSpec));
	role = (RoleSpec *) altersub->newowner;

	memcpy(user->data, role->rolename, NAMEDATALEN);

	PG_RETURN_NAME(user);
}

Datum
ddl_is_change_owner(PG_FUNCTION_ARGS)
{
	bool		ret =
	DDL_CHANGE_OWNER == ddl_alter_table_subcmd(
									(CollectedCommand *) PG_GETARG_POINTER(0)
	);

	PG_RETURN_BOOL(ret);
}
