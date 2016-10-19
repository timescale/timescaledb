CREATE EXTENSION IF NOT EXISTS postgres_fdw;
DROP SERVER IF EXISTS "sys-server" CASCADE;
CREATE SERVER "sys-server" FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'localhost', port '5432', dbname 'system');
CREATE USER MAPPING FOR "postgres" SERVER "sys-server" OPTIONS (user 'postgres', password '');
DROP SCHEMA IF EXISTS "system" CASCADE;
CREATE SCHEMA IF NOT EXISTS "system";
IMPORT FOREIGN SCHEMA public FROM SERVER "sys-server" INTO "system";


CREATE SERVER "local" FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'localhost', port '5432', dbname 'test');
CREATE USER MAPPING FOR "postgres" SERVER "local" OPTIONS (user 'postgres', password '');

DROP SCHEMA IF EXISTS "cluster" CASCADE;
CREATE SCHEMA IF NOT EXISTS "cluster";

CREATE OR REPLACE FUNCTION register_global_table(
	local_table_name text, 
	cluster_table_name text
) RETURNS VOID AS $$
$$
LANGUAGE 'sql' VOLATILE;
