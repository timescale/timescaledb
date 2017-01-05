--data nodes
CREATE TABLE IF NOT EXISTS node (
    database_name NAME    NOT NULL PRIMARY KEY,
    schema_name   NAME    NOT NULL UNIQUE, --public schema of remote
    server_name   NAME    NOT NULL UNIQUE,
    hostname      TEXT    NOT NULL,
    active        BOOLEAN NOT NULL DEFAULT TRUE,
    id            SERIAL  NOT NULL UNIQUE -- id for node. used in naming
);

--singleton holding info about meta db.
CREATE TABLE IF NOT EXISTS meta (
    database_name NAME NOT NULL PRIMARY KEY,
    hostname      TEXT NOT NULL,
    server_name   NAME NOT NULL
);
CREATE UNIQUE INDEX there_can_be_only_one_meta
    ON meta ((1));

--these users should exist an all nodes+meta in the cluster.
CREATE TABLE IF NOT EXISTS cluster_user (
    username TEXT NOT NULL PRIMARY KEY,
    password TEXT NULL --not any more of a security hole than usual since stored in  pg_user_mapping anyway
);

--The hypertable is an abstraction that represents a replicated table that is partition on 2 dimensions.
--One of the dimensions is time, the other is arbitrary.
--This abstraction also tracks the distinct value set of any columns marked as `distinct`.
--Each row creates 3 tables:
--    i) main table is just an alias to the 0'th replica for now. Represents the hypertable to the user.
--    ii) root table is the ancesstor of all the data tables (across replicas). Should not be queryable for data (TODO).
--    iii) distinct root table is the ancestor of all distinct tables (across replicas).  Should not be queryable for data (TODO).
CREATE TABLE IF NOT EXISTS hypertable (
    name                    NAME                  NOT NULL PRIMARY KEY CHECK (name NOT LIKE '\_%'),
    main_schema_name        NAME                  NOT NULL,
    main_table_name         NAME                  NOT NULL,
    associated_schema_name  NAME                  NOT NULL,
    associated_table_prefix NAME                  NOT NULL,
    root_schema_name        NAME                  NOT NULL,
    root_table_name         NAME                  NOT NULL,
    distinct_schema_name    NAME                  NOT NULL,
    distinct_table_name     NAME                  NOT NULL,
    replication_factor      SMALLINT              NOT NULL CHECK (replication_factor > 0),
    placement               chunk_placement_type  NOT NULL,
    time_field_name         NAME                  NOT NULL,
    time_field_type         REGTYPE               NOT NULL,
    created_on              NAME                  NOT NULL REFERENCES node(database_name),
    UNIQUE (main_schema_name, main_table_name),
    UNIQUE (associated_schema_name, associated_table_prefix),
    UNIQUE (root_schema_name, root_table_name)
);

--This represents one replica of the data across all partitions and time.
--Each row creates 2 tables:
--   i)  data replica table (schema_name.table_name)
--       parent is the hypertable root table.
--       childen are the partition_replica tables.
--   ii) distinct replica tables (distinct_schema_name.distinct_table_name)
--       parent is the hypertable distinct root table.
--       childen are created by distinct_replica_node table.
CREATE TABLE IF NOT EXISTS hypertable_replica (
    hypertable_name      NAME     NOT NULL  REFERENCES hypertable (name),
    replica_id           SMALLINT NOT NULL  CHECK (replica_id >= 0),
    schema_name          NAME     NOT NULL,
    table_name           NAME     NOT NULL,
    distinct_schema_name NAME     NOT NULL,
    distinct_table_name  NAME     NOT NULL,
    PRIMARY KEY (hypertable_name, replica_id),
    UNIQUE (schema_name, table_name)
);

--mapping that shows which replica is pointed to by the main table, for each node.
--the translation from main table to replica should happens in C tranformation
--right after the parsing step. (RULES cannot be used, unfortunately)
CREATE TABLE IF NOT EXISTS default_replica_node (
    database_name        NAME NOT NULL  REFERENCES node (database_name),
    hypertable_name      NAME     NOT NULL  REFERENCES hypertable (name),
    replica_id           SMALLINT NOT NULL  CHECK (replica_id >= 0),
    PRIMARY KEY (database_name, hypertable_name),
    FOREIGN KEY (hypertable_name, replica_id) REFERENCES hypertable_replica (hypertable_name, replica_id)
);


--there should be one distinct_replica_node for each node with a chunk from that replica
--so there can be multiple rows for one hypertable-replica on different nodes.
--that way writes are local. Optimized reads are also local for many queries.
--But, some read queries are cross-node.
--Each row creates a table.
--  Parent table:  hypertable_replica.distinct_table
--  No children, created table contains data.
CREATE TABLE IF NOT EXISTS distinct_replica_node (
    hypertable_name NAME     NOT NULL,
    replica_id      SMALLINT NOT NULL,
    database_name   NAME     NOT NULL REFERENCES node (database_name),
    schema_name     NAME     NOT NULL,
    table_name      NAME     NOT NULL,
    PRIMARY KEY (hypertable_name, replica_id, database_name),
    UNIQUE (schema_name, table_name),
    FOREIGN KEY (hypertable_name, replica_id) REFERENCES hypertable_replica (hypertable_name, replica_id)
);

--A partition epoch represents a different partitioning of the data.
--It has a start and end time (data time). Data needs to be placed in the correct epoch by time.
--This should change very infrequently. Expensive to start new epoch.
CREATE TABLE IF NOT EXISTS partition_epoch (
    id                 SERIAL NOT NULL  PRIMARY KEY,
    hypertable_name    NAME   NOT NULL  REFERENCES hypertable (name),
    start_time         BIGINT NULL      CHECK (start_time > 0),
    end_time           BIGINT NULL      CHECK (end_time > 0),
    partitioning_func  NAME   NOT NULL,  --function name of a function of the form func(data_value, partitioning_mod) -> [0, partitioning_mod)
    partitioning_mod   INT    NOT NULL  CHECK (partitioning_mod < 65536),
    partitioning_field NAME   NOT NULL,
    UNIQUE (hypertable_name, start_time),
    UNIQUE (hypertable_name, end_time),
    CHECK (start_time < end_time)
);

-- A partition defines a partition in a partition_epoch.
-- For any partition the keyspace is defined as [0, partition_epoch.partitioning_mod]
-- For any epoch, there must be a partition that covers every element in the keyspace.
CREATE TABLE IF NOT EXISTS partition (
    id             SERIAL   NOT NULL PRIMARY KEY,
    epoch_id       INT      NOT NULL REFERENCES partition_epoch (id),
    keyspace_start SMALLINT NOT NULL CHECK (keyspace_start >= 0), --start inclusive
    keyspace_end   SMALLINT NOT NULL CHECK (keyspace_end > 0), --end   inclusive; compatible with between operator
    UNIQUE (epoch_id, keyspace_start),
    CHECK (keyspace_end > keyspace_start)
);

--Represents a replica for a partition.
--Each row creates a table:
--   Parent: "hypertable_replica.schema_name"."hypertable_replica.table_name"
--   Children: "chunk_replica_node.schema_name"."chunk_replica_node.table_name"
--TODO: trigger to verify partition_epoch hypertable name matches this hypertable_name
CREATE TABLE IF NOT EXISTS partition_replica (
    id              SERIAL   NOT NULL PRIMARY KEY,
    partition_id    INT      NOT NULL REFERENCES partition (id),
    hypertable_name NAME     NOT NULL,
    replica_id      SMALLINT NOT NULL,
    schema_name     NAME     NOT NULL,
    table_name      NAME     NOT NULL,
    UNIQUE (schema_name, table_name),
    UNIQUE (partition_id, replica_id),
    FOREIGN KEY (hypertable_name, replica_id) REFERENCES hypertable_replica (hypertable_name, replica_id)
);

-- Represent a chunk of data
-- i.e. data for a particular hypername-partition for a particular time.
CREATE TABLE IF NOT EXISTS chunk (
    id           SERIAL NOT NULL    PRIMARY KEY,
    partition_id INT    NOT NULL    REFERENCES partition (id),
    start_time   BIGINT NULL        CHECK (start_time > 0),
    end_time     BIGINT NULL        CHECK (end_time > 0),
    UNIQUE (partition_id, start_time),
    UNIQUE (partition_id, end_time),
    CHECK (start_time < end_time)
);

--A mapping between chunks, partition_replica, and node.
--This represents the table where actual data is stored.
--Each row represents a table:
--  Parent table: "partition_replica.schema_name"."partition_replica.table_name"
CREATE TABLE IF NOT EXISTS chunk_replica_node (
    chunk_id             INT  NOT NULL  REFERENCES chunk (id),
    partition_replica_id INT  NOT NULL  REFERENCES partition_replica (id),
    database_name        NAME NOT NULL  REFERENCES node (database_name),
    schema_name          NAME NOT NULL,
    table_name           NAME NOT NULL,
    PRIMARY KEY (chunk_id, partition_replica_id), --a single chunk, replica tuple
    UNIQUE (chunk_id, database_name), --no two chunk replicas on same node
    UNIQUE (schema_name, table_name)
);

--Represents a hypertable field.
--TODO: remove is_partitioning. defined in partition_epoch table.
CREATE TABLE IF NOT EXISTS field (
    hypertable_name NAME                NOT NULL REFERENCES hypertable (name),
    name            NAME                NOT NULL,
    attnum          INT2                NOT NULL, --MUST match pg_attribute.attnum on main table. SHOULD match on root/hierarchy table as well.
    data_type       REGTYPE             NOT NULL,
    default_value   TEXT                NULL,
    is_distinct     BOOLEAN             NOT NULL DEFAULT FALSE,
    not_null        BOOLEAN             NOT NULL,
    created_on      NAME                NOT NULL REFERENCES node(database_name),
    modified_on     NAME                NOT NULL REFERENCES node(database_name),
    PRIMARY KEY (hypertable_name, name),
    UNIQUE(hypertable_name, attnum)
);

CREATE TABLE IF NOT EXISTS deleted_field (
  LIKE field,
  deleted_on NAME
);

CREATE TABLE IF NOT EXISTS hypertable_index (
    hypertable_name  NAME                NOT NULL REFERENCES hypertable (name),
    main_schema_name NAME                NOT NULL, --schema name of main table (needed for a uniqueness constraint)
    main_index_name  NAME                NOT NULL, --index name on main table
    definition       TEXT                NOT NULL, --def with /*INDEX_NAME*/ and /*TABLE_NAME*/ placeholders
    created_on       NAME                NOT NULL REFERENCES node(database_name),
    PRIMARY KEY (hypertable_name, main_index_name),
    UNIQUE(main_schema_name, main_index_name) --globally unique since index names globally unique
);

CREATE TABLE IF NOT EXISTS deleted_hypertable_index (
  LIKE hypertable_index,
  deleted_on NAME
);
