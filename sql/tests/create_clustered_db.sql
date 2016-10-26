DROP DATABASE IF EXISTS meta;
DROP DATABASE IF EXISTS "Test1";
DROP DATABASE IF EXISTS test2;
CREATE DATABASE meta;
CREATE DATABASE "Test1";
CREATE DATABASE test2;

\c meta
\ir load_common.sql
\ir load_meta.sql

\c Test1
\ir load_common.sql
\ir load_main.sql

\c test2
\ir load_common.sql
\ir load_main.sql