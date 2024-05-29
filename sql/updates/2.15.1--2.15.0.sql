CREATE TABLE _timescaledb_catalog._tmp_continuous_aggs_bucket_function AS
    SELECT
      mat_hypertable_id,
      pg_catalog.to_regprocedure(bucket_func) AS bucket_func,
      bucket_width,
      bucket_origin,
      bucket_offset,
      bucket_timezone,
      bucket_fixed_width
    FROM
      _timescaledb_catalog.continuous_aggs_bucket_function
    ORDER BY
         mat_hypertable_id;

ALTER EXTENSION timescaledb
    DROP TABLE _timescaledb_catalog.continuous_aggs_bucket_function;

DROP TABLE _timescaledb_catalog.continuous_aggs_bucket_function;

CREATE TABLE _timescaledb_catalog.continuous_aggs_bucket_function (
  mat_hypertable_id integer NOT NULL,
  -- The bucket function
  bucket_func regprocedure NOT NULL,
  -- `bucket_width` argument of the function, e.g. "1 month"
  bucket_width text NOT NULL,
  -- optional `origin` argument of the function provided by the user
  bucket_origin text,
  -- optional `offset` argument of the function provided by the user
  bucket_offset text,
  -- optional `timezone` argument of the function provided by the user
  bucket_timezone text,
  -- fixed or variable sized bucket
  bucket_fixed_width bool NOT NULL,
  -- table constraints
  CONSTRAINT continuous_aggs_bucket_function_pkey PRIMARY KEY (mat_hypertable_id),
  CONSTRAINT continuous_aggs_bucket_function_mat_hypertable_id_fkey FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE
);

INSERT INTO _timescaledb_catalog.continuous_aggs_bucket_function
  SELECT * FROM _timescaledb_catalog._tmp_continuous_aggs_bucket_function;

DROP TABLE _timescaledb_catalog._tmp_continuous_aggs_bucket_function;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_bucket_function', '');

GRANT SELECT ON TABLE _timescaledb_catalog.continuous_aggs_bucket_function TO PUBLIC;

ANALYZE _timescaledb_catalog.continuous_aggs_bucket_function;
