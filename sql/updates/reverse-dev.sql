-- Update compression settings
CREATE TABLE _timescaledb_catalog.tempsettings (LIKE _timescaledb_catalog.compression_settings);
INSERT INTO _timescaledb_catalog.tempsettings SELECT * FROM _timescaledb_catalog.compression_settings;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.compression_settings;
DROP TABLE _timescaledb_catalog.compression_settings CASCADE;

CREATE TABLE _timescaledb_catalog.compression_settings (
  relid regclass NOT NULL,
  segmentby text[],
  orderby text[],
  orderby_desc bool[],
  orderby_nullsfirst bool[],
  CONSTRAINT compression_settings_pkey PRIMARY KEY (relid),
  CONSTRAINT compression_settings_check_segmentby CHECK (array_ndims(segmentby) = 1),
  CONSTRAINT compression_settings_check_orderby_null CHECK ((orderby IS NULL AND orderby_desc IS NULL AND orderby_nullsfirst IS NULL) OR (orderby IS NOT NULL AND orderby_desc IS NOT NULL AND orderby_nullsfirst IS NOT NULL)),
  CONSTRAINT compression_settings_check_orderby_cardinality CHECK (array_ndims(orderby) = 1 AND array_ndims(orderby_desc) = 1 AND array_ndims(orderby_nullsfirst) = 1 AND cardinality(orderby) = cardinality(orderby_desc) AND cardinality(orderby) = cardinality(orderby_nullsfirst))
);

-- Insert hypertable settings
INSERT INTO _timescaledb_catalog.compression_settings
SELECT
	cs.compress_relid,
	cs.segmentby,
	cs.orderby,
	cs.orderby_desc,
	cs.orderby_nullsfirst
FROM
	_timescaledb_catalog.tempsettings cs
WHERE
	cs.compress_relid IS NOT NULL;

-- Insert chunk settings
INSERT INTO _timescaledb_catalog.compression_settings
SELECT
	cs.relid,
	cs.segmentby,
	cs.orderby,
	cs.orderby_desc,
	cs.orderby_nullsfirst
FROM
	_timescaledb_catalog.tempsettings cs
WHERE
	cs.compress_relid IS NULL;

DROP TABLE _timescaledb_catalog.tempsettings CASCADE;
GRANT SELECT ON _timescaledb_catalog.compression_settings TO PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.compression_settings', '');
