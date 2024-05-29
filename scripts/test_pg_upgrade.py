#!/usr/bin/env python3

import os
import sys

from shutil import rmtree
from testgres import get_new_node, PostgresNode


# Accessor functions
def set_default_conf(node: PostgresNode, unix_socket_dir):
    node.default_conf(fsync=True, allow_streaming=False, allow_logical=False)
    node.append_conf(unix_socket_directories=unix_socket_dir)
    node.append_conf(timezone="GMT")
    node.append_conf(client_min_messages="warning")
    node.append_conf(max_prepared_transactions="100")
    node.append_conf(max_worker_processes="0")
    node.append_conf(shared_preload_libraries="timescaledb")
    node.append_conf("timescaledb.telemetry_level=off")


def write_bytes_to_file(filename, nbytes):
    with open(filename, "wb") as f:
        f.write(nbytes)
        f.close()


def getenv_or_error(envvar):
    return os.getenv(envvar) or sys.exit(f"Environment variable {envvar} not defined")


def getenv_or_default(envvar, default):
    return os.getenv(envvar) or default


def initialize_node(working_dir, prefix, port, bin_dir, base_dir):
    node = get_new_node(prefix=prefix, port=port, bin_dir=bin_dir, base_dir=base_dir)
    node.init()
    set_default_conf(node, working_dir)
    return node


# Globals
pg_version_old = getenv_or_error("PGVERSIONOLD")
pg_version_new = getenv_or_error("PGVERSIONNEW")

pg_node_old = f"pg{pg_version_old}"
pg_node_new = f"pg{pg_version_new}"

pg_port_old = getenv_or_default("PGPORTOLD", "54321")
pg_port_new = getenv_or_default("PGPORTNEW", "54322")

test_version = getenv_or_default("TEST_VERSION", "v8")

pg_bin_old = getenv_or_default("PGBINOLD", f"/usr/lib/postgresql/{pg_version_old}/bin")
pg_bin_new = getenv_or_default("PGBINNEW", f"/usr/lib/postgresql/{pg_version_new}/bin")

working_dir = getenv_or_default(
    "OUTPUT_DIR",
    f"/tmp/pg_upgrade_output/{pg_version_old}_to_{pg_version_new}",
)

pg_data_old = getenv_or_default("PGDATAOLD", f"{working_dir}/{pg_node_old}")
pg_data_new = getenv_or_default("PGDATAOLD", f"{working_dir}/{pg_node_new}")

pg_database_test = getenv_or_default("PGDATABASE", "pg_upgrade_test")

if os.path.exists(working_dir):
    rmtree(working_dir)
os.makedirs(working_dir)

# Real testing code
print(f"Initializing nodes {pg_node_old} and {pg_node_new}")
node_old = initialize_node(
    working_dir, pg_node_old, pg_port_old, pg_bin_old, pg_data_old
)
node_old.start()

node_new = initialize_node(
    working_dir, pg_node_new, pg_port_new, pg_bin_new, pg_data_new
)

print(f"Creating {pg_database_test} database on node {pg_node_old}")
node_old.safe_psql(filename="test/sql/updates/setup.roles.sql")
node_old.safe_psql(query=f"CREATE DATABASE {pg_database_test};")
node_old.safe_psql(dbname=pg_database_test, query="CREATE EXTENSION timescaledb;")
node_old.safe_psql(dbname=pg_database_test, filename="test/sql/updates/pre.testing.sql")
node_old.safe_psql(
    dbname=pg_database_test,
    filename=f"test/sql/updates/setup.{test_version}.sql",
)
node_old.safe_psql(dbname=pg_database_test, query="CHECKPOINT")
node_old.safe_psql(dbname=pg_database_test, filename="test/sql/updates/setup.check.sql")

# Run new psql over the old node to have the same psql output
node_new.port = pg_port_old
(code, old_out, old_err) = node_new.psql(
    dbname=pg_database_test, filename="test/sql/updates/post.pg_upgrade.sql"
)
node_new.port = pg_port_new

# Save output to log
write_bytes_to_file(f"{working_dir}/post.{pg_node_old}.log", old_out)
node_old.stop()

print(f"Upgrading node {pg_node_old} to {pg_node_new}")
res = node_new.upgrade_from(old_node=node_old, options=["--retain"])
node_new.start()

(code, new_out, new_err) = node_new.psql(
    dbname=pg_database_test,
    filename="test/sql/updates/post.pg_upgrade.sql",
)

# Save output to log
write_bytes_to_file(f"{working_dir}/post.pg{pg_version_new}.log", new_out)
node_new.stop()

print(f"Finish upgrading node {pg_node_old} to {pg_node_new}")
