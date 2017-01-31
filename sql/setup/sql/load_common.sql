
-- Can't load extensions from extension
-- \ir ../../common/extensions.sql
CREATE EXTENSION IF NOT EXISTS dblink;
CREATE EXTENSION IF NOT EXISTS postgres_fdw;
CREATE EXTENSION IF NOT EXISTS hstore;

\ir ../../common/types.sql
\ir ../../common/tables.sql
\ir ../../common/cluster_setup_functions.sql
\ir ../../common/chunk.sql
\ir ../../common/util.sql
