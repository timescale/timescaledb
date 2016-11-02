DROP DATABASE IF EXISTS meta;
DROP DATABASE IF EXISTS "Test1";
DROP DATABASE IF EXISTS test2;
CREATE DATABASE meta;
CREATE DATABASE "Test1";
CREATE DATABASE test2;

\c meta
\ir ../../setup/load_common.sql
\ir ../../setup/load_meta.sql

\c Test1
\ir ../../setup/load_common.sql
\ir ../../setup/load_main.sql

\c test2
\ir ../../setup/load_common.sql
\ir ../../setup/load_main.sql