#!/usr/bin/env python

# Check SQL script components for problematic patterns. This script is
# intended to be run on the scripts that are added to every update script,
# but not the compiled update script or the pre_install scripts.
#
# This script will find patterns that are not idempotent and therefore
# should be moved to the pre_install part.

from pglast import parse_sql, parse_plpgsql
from pglast.visitors import Visitor, Skip, Continue
from pglast.stream import RawStream
import sys
import re
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("filename", nargs="+")
args = parser.parse_args()


class SQLVisitor(Visitor):
    def __init__(self, file):
        self.errors = 0
        self.file = file
        super().__init__()

    def error(self, node, hint):
        self.errors += 1
        print(
            f"Invalid statement found in sql script({self.file}):\n",
            RawStream()(node),
        )
        print(hint, "\n")

    def visit_RawStmt(self, _ancestors, _node):
        # Statements are nested in RawStmt so we need to let the visitor descend
        return Continue

    def visit(self, _ancestors, node):
        self.error(node, "Consider moving the statement into a pre_install script")

        # We are only interested in checking top-level statements
        return Skip

    def visit_CommentStmt(self, _ancestors, _node):
        return Skip

    def visit_GrantStmt(self, _ancestors, _node):
        return Skip

    def visit_SelectStmt(self, _ancestors, _node):
        return Skip

    def visit_InsertStmt(self, _ancestors, _node):
        return Skip

    def visit_DeleteStmt(self, _ancestors, _node):
        return Skip

    def visit_DoStmt(self, _ancestors, _node):
        return Skip

    def visit_CreateEventTrigStmt(self, _ancestors, _node):
        return Skip

    def visit_VariableSetStmt(self, _ancestors, node):
        if not node.is_local:
            self.error(node, "Consider using SET LOCAL instead of SET")

        return Skip

    def visit_CreateTrigStmt(self, _ancestors, node):
        if not node.replace:
            self.error(node, "Consider using CREATE OR REPLACE TRIGGER")

        return Skip

    def visit_DefineStmt(self, _ancestors, node):
        if not node.replace:
            self.error(node, "Consider using CREATE OR REPLACE")

        return Skip

    def visit_DropStmt(self, _ancestors, node):
        if not node.missing_ok:
            self.error(node, "Consider using DROP IF EXISTS")

        return Skip

    def visit_ViewStmt(self, _ancestors, node):
        if not node.replace:
            self.error(node, "Consider using CREATE OR REPLACE VIEW")

        return Skip

    def visit_CreateFunctionStmt(self, _ancestors, node):
        if not node.replace:
            fn_str = ("FUNCTION", "PROCEDURE")[node.is_procedure is True]
            self.error(node, f"Consider using CREATE OR REPLACE {fn_str}")

        security = [option for option in node.options if option.defname == "security"]
        if security and security[0].arg.boolval:
            self.error(
                node,
                "SECURITY DEFINER functions created by extension scripts execute "
                "as postgres; "
                "use SECURITY INVOKER and grant the required privileges explicitly",
            )

        return Skip


class FuncCallVisitor(Visitor):
    """Flag function calls that are resolved through search_path at runtime."""

    def __init__(self, file):
        self.errors = 0
        self.file = file
        super().__init__()

    def error(self, node, hint):
        self.errors += 1
        print(
            f"Invalid function call found in sql script({self.file}):\n",
            RawStream()(node),
        )
        print(hint, "\n")

    def visit_query(self, query):
        for stmt in parse_sql(query):
            self(stmt)

    def visit_expr(self, expr):
        self.visit_query(f"SELECT {expr}")

    def visit_assign(self, assign):
        # assignments have the form "target := expr" or "target = expr"
        self.visit_expr(re.sub(r"^[^=]*?:?=", "", assign, count=1))

    def visit_FuncCall(self, _ancestors, node):
        if len(node.funcname) == 1 and node.funcname[0].sval == "format":
            self.error(node, "Use pg_catalog.format()")

    def visit_CreateFunctionStmt(self, _ancestors, node):
        options = node.options or []
        language = [o for o in options if o.defname == "language"]
        body = [o for o in options if o.defname == "as"]
        if language and body and language[0].arg.sval == "sql":
            self.visit_query(body[0].arg[0].sval)


# plpgsql stores expressions as raw text. Most are bare expressions, these
# are full SQL statements and assignments have the target in front.
PLPGSQL_STATEMENTS = {
    ("PLpgSQL_stmt_execsql", "sqlstmt"),
    ("PLpgSQL_stmt_perform", "expr"),
    ("PLpgSQL_stmt_call", "expr"),
    ("PLpgSQL_stmt_fors", "query"),
    ("PLpgSQL_stmt_return_query", "query"),
    ("PLpgSQL_stmt_open", "query"),
    ("PLpgSQL_var", "cursor_explicit_expr"),
}
PLPGSQL_ASSIGNMENT = ("PLpgSQL_stmt_assign", "expr")


def plpgsql_exprs(node, owner=None, field=None):
    """Yield (owner node type, field, query) for all expressions in a plpgsql tree."""
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "PLpgSQL_expr":
                yield owner, field, value["query"]
            elif key.startswith("PLpgSQL_"):
                yield from plpgsql_exprs(value, key, field)
            else:
                yield from plpgsql_exprs(value, owner, key)
    elif isinstance(node, list):
        for item in node:
            yield from plpgsql_exprs(item, owner, field)


# copied from pgspot
def visit_sql(sql, file):
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

    visitor = SQLVisitor(file)
    funccalls = FuncCallVisitor(file)
    for stmt in parse_sql(sql):
        visitor(stmt)
        funccalls(stmt)
    for owner, field, query in plpgsql_exprs(parse_plpgsql(sql)):
        if (owner, field) in PLPGSQL_STATEMENTS:
            funccalls.visit_query(query)
        elif (owner, field) == PLPGSQL_ASSIGNMENT:
            funccalls.visit_assign(query)
        else:
            funccalls.visit_expr(query)
    return visitor.errors + funccalls.errors


def main(args):
    errors = 0
    error_files = []
    for filename in args.filename:
        with open(filename, encoding="utf-8") as file:
            sql = file.read()
        result = visit_sql(sql, filename)
        if result > 0:
            errors += result
            error_files.append(filename)

    if errors > 0:
        numbering = "errors" if errors > 1 else "error"
        print(
            f"{errors} {numbering} detected in {len(error_files)} files({', '.join(error_files)})"
        )
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main(args)
    sys.exit(0)
