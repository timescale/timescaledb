from pglast import parse_sql
from pglast.ast import ColumnDef
from pglast.visitors import Visitor
from pglast import enums
import sys
import re
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("filename")
parser.add_argument("--latest", action="store_true", help="process latest-dev.sql")
args = parser.parse_args()


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
    # CREATE TABLE IF NOT EXISTS ..
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
        if node.if_not_exists:
            self.errors += 1
            print(
                f"ERROR: Attempting to CREATE TABLE IF NOT EXISTS {node.relation.relname}"
            )

        # We have to be careful with the column types we use in our catalog to only allow types
        # that are safe to use in catalog tables and not cause problems during extension upgrade,
        # pg_upgrade or dump/restore.
        if node.tableElts is not None:
            for coldef in node.tableElts:
                if isinstance(coldef, ColumnDef):
                    if coldef.typeName.arrayBounds is not None:
                        if coldef.typeName.names[-1].sval not in [
                            "bool",
                            "text",
                        ]:
                            self.errors += 1
                            print(
                                f"ERROR: Attempting to CREATE TABLE {node.relation.relname} with blocked array type {coldef.typeName.names[-1].sval}"
                            )
                    else:
                        if coldef.typeName.names[-1].sval not in [
                            "bool",
                            "int2",
                            "int4",
                            "int8",
                            "interval",
                            "jsonb",
                            "name",
                            "regclass",
                            "regtype",
                            "regrole",
                            "serial",
                            "text",
                            "timestamptz",
                        ]:
                            self.errors += 1
                            print(
                                f"ERROR: Attempting to CREATE TABLE {node.relation.relname} with blocked type {coldef.typeName.names[-1].sval}"
                            )

    # CREATE SCHEMA IF NOT EXISTS ..
    def visit_CreateSchemaStmt(
        self, ancestors, node
    ):  # pylint: disable=unused-argument
        if node.if_not_exists:
            self.errors += 1
            print(f"ERROR: Attempting to CREATE SCHEMA IF NOT EXISTS {node.schemaname}")

    # CREATE MATERIALIZED VIEW IF NOT EXISTS ..
    def visit_CreateTableAsStmt(
        self, ancestors, node
    ):  # pylint: disable=unused-argument
        if node.if_not_exists:
            self.errors += 1
            print(
                f"ERROR: Attempting to CREATE MATERIALIZED VIEW IF NOT EXISTS {node.into.rel.relname}"
            )

    # CREATE FOREIGN TABLE IF NOT EXISTS ..
    def visit_CreateForeignTableStmt(
        self, ancestors, node
    ):  # pylint: disable=unused-argument
        if node.base.if_not_exists:
            self.errors += 1
            print(
                f"ERROR: Attempting to CREATE FOREIGN TABLE IF NOT EXISTS {node.base.relation.relname}"
            )

    # CREATE INDEX IF NOT EXISTS ..
    def visit_IndexStmt(self, ancestors, node):  # pylint: disable=unused-argument
        if node.if_not_exists:
            self.errors += 1
            print(f"ERROR: Attempting to CREATE INDEX IF NOT EXISTS {node.idxname}")

    # CREATE FUNCTION / PROCEDURE _timescaledb_internal...
    def visit_CreateFunctionStmt(
        self, ancestors, node
    ):  # pylint: disable=unused-argument
        if args.latest:
            # C functions should only appear in actual function definition but not
            # in latest-dev.sql as that would introduce a dependency on the library.
            # In that case, we want to use a dedicated placeholder function.
            lang = [elem for elem in node.options if elem.defname == "language"]
            code = [elem for elem in node.options if elem.defname == "as"][0].arg
            if (
                lang
                and lang[0].arg.sval == "c"
                and code[-1].sval != "ts_update_placeholder"
            ):
                self.errors += 1
                functype = "procedure" if node.is_procedure else "function"
                print(
                    f"ERROR: Attempting to create {functype} {node.funcname[1].sval} with language 'c'"
                )

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


def main(args):
    file = args.filename

    with open(file, "r", encoding="utf-8") as f:
        sql = f.read()
        errors = visit_sql(sql)
        if errors > 0:
            numbering = "errors" if errors > 1 else "error"
            print(f"{errors} {numbering} detected in file {file}")
            sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main(args)
    sys.exit(0)
