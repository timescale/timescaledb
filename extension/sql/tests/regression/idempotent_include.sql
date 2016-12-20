\set ON_ERROR_STOP 1

\ir create_clustered_db.sql

\c meta
\ir ../../setup/sql/load_common.sql
\ir ../../setup/sql/load_meta.sql

\c Test1
\ir ../../setup/sql/load_common.sql
\ir ../../setup/sql/load_main.sql

\c test2
\ir ../../setup/sql/load_common.sql
\ir ../../setup/sql/load_main.sql
