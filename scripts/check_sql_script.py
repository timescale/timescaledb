#!/usr/bin/env python

# Check SQL script components for problematic patterns. This script is
# intended to be run on the scripts that are added to every update script,
# but not the compiled update script or the pre_install scripts.
#
# This script will find patterns that are not idempotent and therefore
# should be moved to the pre_install part.

from pglast import parse_sql
from pglast.visitors import Visitor, Skip, Continue
from pglast.stream import RawStream
import sys
import re
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("filename", type=argparse.FileType("r"), nargs="+")
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

        return Skip


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
    for stmt in parse_sql(sql):
        visitor(stmt)
    return visitor.errors


def main(args):
    errors = 0
    error_files = []
    for file in args.filename:
        sql = file.read()
        result = visit_sql(sql, file.name)
        if result > 0:
            errors += result
            error_files.append(file.name)

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
