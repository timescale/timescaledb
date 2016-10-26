\set ON_ERROR_STOP 1

\ir create_clustered_db.sql

\c meta
\ir load_common.sql
\ir load_meta.sql

\c Test1
\ir load_common.sql
\ir load_main.sql

\c test2
\ir load_common.sql
\ir load_main.sql