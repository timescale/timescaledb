DO $$
BEGIN
    CREATE ROLE alt_usr LOGIN;
EXCEPTION
    WHEN duplicate_object THEN
        --mute error
END$$;

--needed for ddl ops:
CREATE SCHEMA IF NOT EXISTS "customSchema" AUTHORIZATION alt_usr;

--test creating and using schema as non-superuser
\c single alt_usr
