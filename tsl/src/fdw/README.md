# Query planning and execution for distributed hypertables

The code in this directory deals with the planning and execution of
queries and inserts on distributed hypertables. The code is based on
PostgreSQL's `postgres_fdw`-- the foreign data wrapper implementation
for querying tables on remote PostgreSQL servers. While we rely on the
same basic foreign data wrapper (FDW) API for interfacing with the
main PostgreSQL planner and executor, we don't consider us strictly
bound to this interface. Therefore, the `timescaledb_fdw`
implementation is not to be considered a regular stand-alone foreign
data wrapper in that you can't manually create foreign tables of that
type. Instead, the use of the FDW interface is out of necessity and is
a transparent part of distributed hypertables.

The code is roughly split along planning and execution lines, and
various utilities:

* `fdw.c`: Implements the foreign data wrapper interface (FDW). This
  is just a thin layer that calls into other code.
* `modify_(plan|exec).c`: Planning and execution of inserts, updates,
  deletes. Note, however, that inserts are mainly handled by
  `data_node_dispatch.c`, which optimizes for batched inserts on
  distributed hypertables.
* `scan_(plan|exec).c`: General planning and execution of remote
  relation scans.
* `relinfo.c`: Information about a remote relation, which is used for
  planning distributed queries/inserts. This can be considered an
  extension of a standard `RelOptInfo` object.
* `estimate.c`: Code for estimating the cost of scanning distributed
  hypertables and chunks.
* `option.c`: Parsing and validation of options on servers, tables,
  extension levels that are related to distributed queries and
  inserts.
* `deparse.c`: Code to generate remote SQL queries from query
  plans. The generated SQL statements are sent to remote data node
  servers.
* `shippable.c`: Determines whether expressions in queries are
  shippable to the remote end. Certain functions are not safe to
  execute on a remote data node or might not exist there.
* `data_node_scan_(plan|exec).c`: Code to turn per-chunk plans into
  per-server plans for more efficient execution.
* `data_node_chunk_assignment.c`: Methods to assign/schedule chunks on
  data node servers.
