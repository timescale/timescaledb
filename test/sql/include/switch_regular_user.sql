DO $$
BEGIN
    CREATE ROLE alt_usr LOGIN;
EXCEPTION
    WHEN duplicate_object THEN
        --mute error
END$$;

--test creating and using schema as non-superuser
\c single alt_usr
