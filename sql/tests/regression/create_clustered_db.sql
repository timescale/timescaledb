DROP DATABASE IF EXISTS meta;
DROP DATABASE IF EXISTS "Test1";
DROP DATABASE IF EXISTS test2;
CREATE DATABASE meta;
CREATE DATABASE "Test1";
CREATE DATABASE test2;

\c meta
\ir ../../setup/sql/load_common.sql
\ir ../../setup/sql/load_meta.sql

\c Test1
\ir ../../setup/sql/load_common.sql
\ir ../../setup/sql/load_main.sql

\c test2
\ir ../../setup/sql/load_common.sql
\ir ../../setup/sql/load_main.sql
