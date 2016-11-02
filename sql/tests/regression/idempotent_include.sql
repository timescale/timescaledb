\set ON_ERROR_STOP 1

\ir create_clustered_db.sql

\c meta
\ir ../../setup/load_common.sql
\ir ../../setup/load_meta.sql

\c Test1
\ir ../../setup/load_common.sql
\ir ../../setup/load_main.sql

\c test2
\ir ../../setup/load_common.sql
\ir ../../setup/load_main.sql