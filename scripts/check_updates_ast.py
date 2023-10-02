from pglast import parse_sql
from pglast.visitors import Visitor
from pglast import enums
import sys
import re


class SQLVisitor(Visitor):
    def __init__(self):
        self.errors = 0
        self.catalog_schemata = [
            "_timescaledb_catalog",
            "_timescaledb_config",
            "_timescaledb_internal",
        ]
        super().__init__()

    # ALTER TABLE _timescaledb_catalog.<tablename> ADD/DROP COLUMN
    def visit_AlterTableStmt(self, ancestors, node):  # pylint: disable=unused-argument
        if (
            "schemaname" in node.relation
            and node.relation.schemaname in self.catalog_schemata
        ):
            schema = node.relation.schemaname
            table = node.relation.relname
            for cmd in node.cmds:
                if cmd.subtype in (
                    enums.AlterTableType.AT_AddColumn,
                    enums.AlterTableType.AT_DropColumn,
                ):
                    self.errors += 1
                    if cmd.subtype == enums.AlterTableType.AT_AddColumn:
                        column = cmd.def_.colname
                        print(
                            f"ERROR: Attempting to ADD COLUMN {column} to catalog table {schema}.{table}"
                        )
                    else:
                        column = cmd.name
                        print(
                            f"ERROR: Attempting to DROP COLUMN {column} from catalog table {schema}.{table}"
                        )

    # ALTER TABLE _timescaledb_catalog.<tablename> RENAME TO
    def visit_RenameStmt(self, ancestors, node):  # pylint: disable=unused-argument
        if (
            node.renameType == enums.ObjectType.OBJECT_TABLE
            and node.relation.schemaname in self.catalog_schemata
        ):
            self.errors += 1
            print(
                f"ERROR: Attempting to RENAME catalog table {node.relation.schemaname}.{node.relation.relname}"
            )

    # CREATE TEMP | TEMPORARY TABLE ..
    def visit_CreateStmt(self, ancestors, node):  # pylint: disable=unused-argument
        if node.relation.relpersistence == "t":
            self.errors += 1
            schema = (
                node.relation.schemaname + "."
                if node.relation.schemaname is not None
                else ""
            )
            print(
                f"ERROR: Attempting to CREATE TEMPORARY TABLE {schema}{node.relation.relname}"
            )

    # CREATE FUNCTION / PROCEDURE _timescaledb_internal...
    def visit_CreateFunctionStmt(
        self, ancestors, node
    ):  # pylint: disable=unused-argument
        if len(node.funcname) == 2 and node.funcname[0].sval == "_timescaledb_internal":
            self.errors += 1
            functype = "procedure" if node.is_procedure else "function"
            print(
                f"ERROR: Attempting to create {functype} {node.funcname[1].sval} in the internal schema"
            )


# copied from pgspot
def visit_sql(sql):
    # @extschema@ is placeholder in extension scripts for
    # the schema the extension gets installed in
    sql = sql.replace("@extschema@", "extschema")
    sql = sql.replace("@extowner@", "extowner")
    sql = sql.replace("@database_owner@", "database_owner")
    # postgres contrib modules are protected by psql meta commands to
    # prevent running extension files in psql.
    # The SQL parser will error on those since they are not valid
    # SQL, so we comment out all psql meta commands before parsing.
    sql = re.sub(r"^\\", "-- \\\\", sql, flags=re.MULTILINE)

    visitor = SQLVisitor()
    # try:
    for stmt in parse_sql(sql):
        visitor(stmt)
    return visitor.errors


def main():
    file = sys.argv[1]
    with open(file, "r", encoding="utf-8") as f:
        sql = f.read()
        errors = visit_sql(sql)
        if errors > 0:
            numbering = "errors" if errors > 1 else "error"
            print(f"{errors} {numbering} detected in file {file}")
            sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
    sys.exit(0)
