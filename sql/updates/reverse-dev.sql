DROP FUNCTION IF EXISTS @extschema@.time_bucket_gapfill(bucket_width SMALLINT, ts SMALLINT, "offset" SMALLINT, start SMALLINT, finish SMALLINT);

DROP FUNCTION IF EXISTS @extschema@.time_bucket_gapfill(bucket_width INT, ts INT, "offset" INT, start INT, finish INT);

DROP FUNCTION IF EXISTS @extschema@.time_bucket_gapfill(bucket_width BIGINT, ts BIGINT, "offset" BIGINT, start BIGINT, finish BIGINT);

DROP FUNCTION IF EXISTS @extschema@.time_bucket_gapfill(bucket_width INTERVAL, ts DATE, "offset" INTERVAL, origin DATE, start DATE, finish DATE);

DROP FUNCTION IF EXISTS @extschema@.time_bucket_gapfill(bucket_width INTERVAL, ts TIMESTAMP, "offset" INTERVAL, origin TIMESTAMP, start TIMESTAMP, finish TIMESTAMP);

DROP FUNCTION IF EXISTS @extschema@.time_bucket_gapfill(bucket_width INTERVAL, ts TIMESTAMPTZ, "offset" INTERVAL, origin TIMESTAMPTZ, start TIMESTAMPTZ, finish TIMESTAMPTZ);

DROP FUNCTION IF EXISTS @extschema@.time_bucket_gapfill(bucket_width INTERVAL, ts TIMESTAMPTZ, timezone TEXT, "offset" INTERVAL, origin TIMESTAMPTZ, start TIMESTAMPTZ, finish TIMESTAMPTZ);
