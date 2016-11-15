CREATE TABLE IF NOT EXISTS node (
    database_name NAME PRIMARY KEY NOT NULL,
    schema_name   NAME UNIQUE      NOT NULL, --public schema of remote
    server_name   NAME UNIQUE      NOT NULL,
    hostname      TEXT             NOT NULL,
    active        BOOLEAN          NOT NULL DEFAULT TRUE,
    id            SERIAL           UNIQUE -- id for node. used in naming
);

CREATE TABLE IF NOT EXISTS cluster_user (
    username TEXT PRIMARY KEY NOT NULL,
    password TEXT --not any more of a security hole than usual since stored in  pg_user_mapping anyway
);


--The hypertable is an abstraction that represents a replicated table that is partition on 2 dimensions.
--One of the dimensions is time, the other is arbitrary.
--This abstraction also tracks the distinct value set of any columns marked as `distinct`.
CREATE TABLE IF NOT EXISTS hypertable (
    name                        NAME      NOT NULL PRIMARY KEY CHECK(name NOT LIKE '\_%'),
    main_schema_name            NAME      NOT NULL,
    main_table_name             NAME      NOT NULL,
    associated_schema_name      NAME      NOT NULL,
    associated_table_prefix     NAME      NOT NULL,
    root_schema_name            NAME      NOT NULL,
    root_table_name             NAME      NOT NULL,
    distinct_schema_name        NAME      NOT NULL,
    distinct_table_name         NAME      NOT NULL, 
    replication_factor          SMALLINT  NOT NULL CHECK(replication_factor > 0), 
    UNIQUE (main_schema_name, main_table_name),
    UNIQUE (associated_schema_name, associated_table_prefix),
    UNIQUE (root_schema_name, root_table_name)
);

CREATE TABLE IF NOT EXISTS hypertable_replica (
    hypertable_name       NAME      NOT NULL  REFERENCES hypertable (name),
    replica_id            SMALLINT  NOT NULL  CHECK(replica_id >= 0),
    schema_name           NAME      NOT NULL,
    table_name            NAME      NOT NULL,
    distinct_schema_name  NAME      NOT NULL,
    distinct_table_name   NAME      NOT NULL,
    PRIMARY KEY (hypertable_name, replica_id),
    UNIQUE (schema_name, table_name)
);


--there should be one distinct_replica_node for each node with a chunk from that replica
--so there can be multiple rows for one hypertable-replica on different nodes.
--that way writes are local. Optimized reads are also local for many queries.
--But, some read queries are cross-node.
CREATE TABLE IF NOT EXISTS distinct_replica_node (
    hypertable_name     NAME      NOT NULL,
    replica_id          SMALLINT  NOT NULL,
    database_name       NAME      NOT NULL REFERENCES node(database_name),
    schema_name         NAME      NOT NULL,
    table_name          NAME      NOT NULL,
    PRIMARY KEY (hypertable_name, replica_id, database_name),
    UNIQUE (schema_name, table_name),
    FOREIGN KEY (hypertable_name, replica_id) REFERENCES hypertable_replica(hypertable_name, replica_id)
);

CREATE TABLE IF NOT EXISTS partition_epoch (
  id                  SERIAL    PRIMARY KEY,
  hypertable_name     NAME      NOT NULL REFERENCES hypertable (name),
  start_time          BIGINT    CHECK(start_time > 0),
  end_time            BIGINT    CHECK(end_time > 0),
  partitioning_func   NAME      NOT NULL,
  partitioning_mod    INT       NOT NULL CHECK(partitioning_mod < 65536), 
  partitioning_field  NAME      NOT NULL,
  UNIQUE(hypertable_name, start_time),
  UNIQUE(hypertable_name, end_time),
  CHECK(start_time < end_time)
);

CREATE TABLE IF NOT EXISTS partition (
  id              SERIAL    PRIMARY KEY,
  epoch_id        INT       NOT NULL REFERENCES partition_epoch(id),
  keyspace_start  SMALLINT  NOT NULL CHECK(keyspace_start >= 0), --start inclusive
  keyspace_end    SMALLINT  NOT NULL CHECK(keyspace_end > 0),    --end   inclusive; compatible with between operator
  UNIQUE(epoch_id, keyspace_start),
  CHECK(keyspace_end > keyspace_start)
);

--todo: trigger to verify partition_epoch hypertable name matches this hypertable_name
CREATE TABLE IF NOT EXISTS partition_replica (
  id              SERIAL    PRIMARY KEY,
  partition_id    INT       NOT NULL REFERENCES partition(id),
  hypertable_name NAME      NOT NULL,
  replica_id      SMALLINT  NOT NULL,
  schema_name     NAME      NOT NULL,
  table_name      NAME      NOT NULL,
  UNIQUE(schema_name, table_name),
  FOREIGN KEY (hypertable_name, replica_id) REFERENCES hypertable_replica(hypertable_name, replica_id)
);


CREATE TABLE IF NOT EXISTS chunk (
  id              SERIAL            PRIMARY KEY,
  partition_id    INT     NOT NULL  REFERENCES partition(id),
  start_time      BIGINT            CHECK(start_time > 0),
  end_time        BIGINT            CHECK(end_time > 0),
  UNIQUE(partition_id, start_time),
  UNIQUE(partition_id, end_time),
  CHECK(start_time < end_time)
);


--mapping between chunks, partition_replica, and node
CREATE TABLE IF NOT EXISTS chunk_replica_node (
  chunk_id             INT      NOT NULL  REFERENCES chunk(id),
  partition_replica_id INT      NOT NULL  REFERENCES partition_replica(id), 
  database_name       NAME      NOT NULL  REFERENCES node(database_name),
  schema_name         NAME      NOT NULL,
  table_name          NAME      NOT NULL,
  PRIMARY KEY (chunk_id, partition_replica_id), --a single chunk, replica tuple
  UNIQUE (chunk_id, database_name), --no two chunk replicas on same node
  UNIQUE (schema_name, table_name)
);

CREATE TABLE IF NOT EXISTS field (
    hypertable_name   NAME                NOT NULL REFERENCES hypertable (name),
    name              NAME                NOT NULL,
    data_type         REGTYPE             NOT NULL,
    is_partitioning   BOOLEAN             NOT NULL DEFAULT FALSE,
    is_distinct       BOOLEAN             NOT NULL DEFAULT FALSE,
    index_types       field_index_type [] NOT NULL,
    PRIMARY KEY (hypertable_name, name)
);

CREATE UNIQUE INDEX IF NOT EXISTS one_partition_field
    ON field (hypertable_name)
    WHERE is_partitioning;
