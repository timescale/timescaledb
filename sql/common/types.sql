DO
$BODY$
BEGIN
    IF NOT EXISTS(SELECT 1
                  FROM pg_type
                  WHERE typname = 'field_index_type') THEN
        CREATE TYPE field_index_type AS ENUM ('TIME-VALUE', 'VALUE-TIME');
    END IF;
END
$BODY$;
