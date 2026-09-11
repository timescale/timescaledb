-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SET timezone TO 'UTC';
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-- Helper to inspect the granular refresh configuration of a hypertable.
\set GRC 'SELECT h.table_name, granular_refresh_column, granular_refresh_start_offset, granular_refresh_end_offset FROM _timescaledb_catalog.hypertable_cagg_settings s JOIN _timescaledb_catalog.hypertable h ON h.id = s.hypertable_id WHERE h.table_name = '

----------------------------------------------------------------------
-- ALTER TABLE <hypertable> SET (timescaledb.granular_refresh_*)
----------------------------------------------------------------------

CREATE TABLE metrics (time timestamptz NOT NULL, device_id integer, value float8);
SELECT create_hypertable('metrics', 'time', chunk_time_interval => '1 day'::interval);

-- Not configured by default
:GRC 'metrics';

\set ON_ERROR_STOP 0
-- Error: all three options are required to enable granular refresh.
ALTER TABLE metrics SET (timescaledb.granular_refresh_column = 'device_id');
ALTER TABLE metrics SET (timescaledb.granular_refresh_start_offset = '2 months 30 days');
ALTER TABLE metrics SET (
    timescaledb.granular_refresh_column = 'device_id',
    timescaledb.granular_refresh_start_offset = '2 months 30 days'
);
-- Error: column does not exist.
ALTER TABLE metrics SET (
    timescaledb.granular_refresh_column = 'does_not_exist',
    timescaledb.granular_refresh_start_offset = '2 months 30 days',
    timescaledb.granular_refresh_end_offset = '5 days'
);
-- Error: column type is not supported (must be timestamp, date, integer, UUID
-- or a string type).
ALTER TABLE metrics SET (
    timescaledb.granular_refresh_column = 'value',
    timescaledb.granular_refresh_start_offset = '2 months 30 days',
    timescaledb.granular_refresh_end_offset = '5 days'
);
-- Error: the column cannot be empty (disabling is not supported).
ALTER TABLE metrics SET (
    timescaledb.granular_refresh_column = '',
    timescaledb.granular_refresh_start_offset = '2 months 30 days',
    timescaledb.granular_refresh_end_offset = '5 days'
);
-- Error: start_offset must be greater than end_offset.
ALTER TABLE metrics SET (
    timescaledb.granular_refresh_column = 'device_id',
    timescaledb.granular_refresh_start_offset = '5 days',
    timescaledb.granular_refresh_end_offset = '2 months 30 days'
);
-- Error: offsets must not be negative.
ALTER TABLE metrics SET (
    timescaledb.granular_refresh_column = 'device_id',
    timescaledb.granular_refresh_start_offset = '-2 months 30 days',
    timescaledb.granular_refresh_end_offset = '5 days'
);
ALTER TABLE metrics SET (
    timescaledb.granular_refresh_column = 'device_id',
    timescaledb.granular_refresh_start_offset = '2 months 30 days',
    timescaledb.granular_refresh_end_offset = '-5 days'
);
-- Error: NULL is not a valid offset value.
ALTER TABLE metrics SET (
    timescaledb.granular_refresh_column = 'device_id',
    timescaledb.granular_refresh_start_offset = NULL,
    timescaledb.granular_refresh_end_offset = '5 days'
);
ALTER TABLE metrics SET (
    timescaledb.granular_refresh_column = 'device_id',
    timescaledb.granular_refresh_start_offset = '2 months 30 days',
    timescaledb.granular_refresh_end_offset = NULL
);
-- Error: timescaledb options only apply to hypertables.
CREATE TABLE plain_table (time timestamptz NOT NULL, device_id integer);
ALTER TABLE plain_table SET (
    timescaledb.granular_refresh_column = 'device_id',
    timescaledb.granular_refresh_start_offset = '2 months 30 days',
    timescaledb.granular_refresh_end_offset = '5 days'
);
\set ON_ERROR_STOP 1
DROP TABLE plain_table;

-- Nothing was configured by the failed attempts.
:GRC 'metrics';

-- Enable granular refresh: the column and the late-arrival window.
ALTER TABLE metrics SET (
    timescaledb.granular_refresh_column = 'device_id',
    timescaledb.granular_refresh_start_offset = '2 months 30 days',
    timescaledb.granular_refresh_end_offset = '5 days'
);
:GRC 'metrics';

-- Once configured, the offsets can be changed, either both at once
-- or separately.

--Both at once
ALTER TABLE metrics SET (
    timescaledb.granular_refresh_start_offset = '30 days',
    timescaledb.granular_refresh_end_offset = '1 day'
);
:GRC 'metrics';

-- The start offset alone; the end offset keeps its stored value.
ALTER TABLE metrics SET (timescaledb.granular_refresh_start_offset = '20 days');
:GRC 'metrics';

-- The end offset alone; the start offset keeps its stored value.
ALTER TABLE metrics SET (timescaledb.granular_refresh_end_offset = '2 days');
:GRC 'metrics';

-- Repeating the column it already has is accepted, so the whole configuration
-- can be restated with one offset changed.
ALTER TABLE metrics SET (
    timescaledb.granular_refresh_column = 'device_id',
    timescaledb.granular_refresh_start_offset = '25 days',
    timescaledb.granular_refresh_end_offset = '2 days'
);
:GRC 'metrics';

-- Setting the column to the same value as the current one is accepted as
-- a no-op.
ALTER TABLE metrics SET (timescaledb.granular_refresh_column = 'device_id');
:GRC 'metrics';

\set ON_ERROR_STOP 0
-- Error: a new offset is checked against the stored one, so setting the start
-- offset alone below the stored end offset is rejected.
ALTER TABLE metrics SET (timescaledb.granular_refresh_start_offset = '1 day');

-- Error: and the same from the other side -- an end offset equal to the stored
-- start offset leaves an empty window.
ALTER TABLE metrics SET (timescaledb.granular_refresh_end_offset = '25 days');

-- Error: the column cannot be changed.
ALTER TABLE metrics SET (timescaledb.granular_refresh_column = 'value');

-- Error: Cannot change the column to an empty string.
ALTER TABLE metrics SET (timescaledb.granular_refresh_column = '');

-- Error: a rejected update leaves the stored offsets untouched, so this one
-- still fails against the same stored end offset.
ALTER TABLE metrics SET (timescaledb.granular_refresh_start_offset = '1 day');

-- Error: a different column is rejected in a whole statement too, and the
-- offsets alongside it are not applied either.
ALTER TABLE metrics SET (
    timescaledb.granular_refresh_column = 'value',
    timescaledb.granular_refresh_start_offset = '20 days',
    timescaledb.granular_refresh_end_offset = '2 days'
);

\set ON_ERROR_STOP 1
-- None of the failed attempts changed anything.
:GRC 'metrics';

-- Integer-time hypertable: offsets are interpreted as integers.
CREATE TABLE metrics_int (time bigint NOT NULL, sensor integer, value float8);
SELECT create_hypertable('metrics_int', 'time', chunk_time_interval => 100000);
\set ON_ERROR_STOP 0
-- Error: offsets must not be negative.
ALTER TABLE metrics_int SET (
    timescaledb.granular_refresh_column = 'sensor',
    timescaledb.granular_refresh_start_offset = 50000,
    timescaledb.granular_refresh_end_offset = -1000
);
-- Error: NULL is not a valid offset value.
ALTER TABLE metrics_int SET (
    timescaledb.granular_refresh_column = 'sensor',
    timescaledb.granular_refresh_start_offset = NULL,
    timescaledb.granular_refresh_end_offset = 1000
);
ALTER TABLE metrics_int SET (
    timescaledb.granular_refresh_column = 'sensor',
    timescaledb.granular_refresh_start_offset = 50000,
    timescaledb.granular_refresh_end_offset = NULL
);
ALTER TABLE metrics_int SET (
    timescaledb.granular_refresh_column = 'sensor',
    timescaledb.granular_refresh_start_offset = 50000,
    timescaledb.granular_refresh_end_offset = 1000
);
:GRC 'metrics_int';

-- Test for integer offsets updates
ALTER TABLE metrics_int SET (timescaledb.granular_refresh_end_offset = 2000);
:GRC 'metrics_int';
ALTER TABLE metrics_int SET (timescaledb.granular_refresh_start_offset = 40000);
:GRC 'metrics_int';
-- Error: checked against the stored end offset, which is now 2000.
ALTER TABLE metrics_int SET (timescaledb.granular_refresh_start_offset = 1000);
:GRC 'metrics_int';

DROP TABLE metrics_int;
DROP TABLE metrics;

----------------------------------------------------------------------
-- ALTER MATERIALIZED VIEW <cagg> SET (timescaledb.enable_granular_refresh)
----------------------------------------------------------------------

CREATE TABLE sensors (time timestamptz NOT NULL, sensor_id integer, temp float8);
SELECT create_hypertable('sensors', 'time', chunk_time_interval => '1 day'::interval);

CREATE MATERIALIZED VIEW sensors_hourly
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 hour', time) AS bucket, sensor_id, avg(temp) AS avg_temp
FROM sensors
GROUP BY bucket, sensor_id
WITH NO DATA;

\set GRE 'SELECT user_view_name, granular_refresh_enabled FROM _timescaledb_catalog.continuous_agg WHERE user_view_name = '

-- Disabled by default.
:GRE 'sensors_hourly';

\set ON_ERROR_STOP 0
-- Error: the option is not supported in CREATE MATERIALIZED VIEW, regardless
-- of the value.
CREATE MATERIALIZED VIEW sensors_hourly_oncreate
WITH (timescaledb.continuous, timescaledb.enable_granular_refresh = true) AS
SELECT time_bucket('1 hour', time) AS bucket, sensor_id, avg(temp) AS avg_temp
FROM sensors
GROUP BY bucket, sensor_id
WITH NO DATA;
CREATE MATERIALIZED VIEW sensors_hourly_oncreate
WITH (timescaledb.continuous, timescaledb.enable_granular_refresh = false) AS
SELECT time_bucket('1 hour', time) AS bucket, sensor_id, avg(temp) AS avg_temp
FROM sensors
GROUP BY bucket, sensor_id
WITH NO DATA;
-- Error: the raw hypertable has no granular refresh configuration yet.
ALTER MATERIALIZED VIEW sensors_hourly SET (timescaledb.enable_granular_refresh = true);
\set ON_ERROR_STOP 1

-- Configure granular refresh on the raw hypertable, then enable it on the cagg.
ALTER TABLE sensors SET (
    timescaledb.granular_refresh_column = 'sensor_id',
    timescaledb.granular_refresh_start_offset = '2 months 30 days',
    timescaledb.granular_refresh_end_offset = '5 days'
);
ALTER MATERIALIZED VIEW sensors_hourly SET (timescaledb.enable_granular_refresh = true);
:GRE 'sensors_hourly';

-- Enabling again is a no-op, not an error.
ALTER MATERIALIZED VIEW sensors_hourly SET (timescaledb.enable_granular_refresh = true);
:GRE 'sensors_hourly';

-- Disabling flips the flag back; the hypertable configuration is left alone.
ALTER MATERIALIZED VIEW sensors_hourly SET (timescaledb.enable_granular_refresh = false);
:GRE 'sensors_hourly';
SELECT granular_refresh_column FROM _timescaledb_catalog.hypertable_cagg_settings
WHERE hypertable_id = (SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name = 'sensors');

-- Disabling again is a no-op, not an error.
ALTER MATERIALIZED VIEW sensors_hourly SET (timescaledb.enable_granular_refresh = false);
:GRE 'sensors_hourly';

-- Re-enabling works.
ALTER MATERIALIZED VIEW sensors_hourly SET (timescaledb.enable_granular_refresh = true);
:GRE 'sensors_hourly';

DROP MATERIALIZED VIEW sensors_hourly;
DROP TABLE sensors;

-- Error: the granular refresh column must be one of the cagg's grouping columns.
CREATE TABLE readings (time timestamptz NOT NULL, sensor_id integer, location text, temp float8);
SELECT create_hypertable('readings', 'time', chunk_time_interval => '1 day'::interval);
ALTER TABLE readings SET (
    timescaledb.granular_refresh_column = 'sensor_id',
    timescaledb.granular_refresh_start_offset = '2 months 30 days',
    timescaledb.granular_refresh_end_offset = '5 days'
);

CREATE MATERIALIZED VIEW readings_by_location
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 hour', time) AS bucket, location, avg(temp) AS avg_temp
FROM readings
GROUP BY bucket, location
WITH NO DATA;

\set ON_ERROR_STOP 0
ALTER MATERIALIZED VIEW readings_by_location SET (timescaledb.enable_granular_refresh = true);
\set ON_ERROR_STOP 1
:GRE 'readings_by_location';

DROP MATERIALIZED VIEW readings_by_location;
DROP TABLE readings;

-- Error: only one cagg per hypertable can enable granular refresh
CREATE TABLE devices (time timestamptz NOT NULL, device_id integer, value float8);
SELECT create_hypertable('devices', 'time', chunk_time_interval => '1 day'::interval);
ALTER TABLE devices SET (
    timescaledb.granular_refresh_column = 'device_id',
    timescaledb.granular_refresh_start_offset = '2 months 30 days',
    timescaledb.granular_refresh_end_offset = '5 days'
);

CREATE MATERIALIZED VIEW devices_hourly
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 hour', time) AS bucket, device_id, avg(value) AS avg_value
FROM devices
GROUP BY bucket, device_id
WITH NO DATA;

CREATE MATERIALIZED VIEW devices_daily
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', time) AS bucket, device_id, avg(value) AS avg_value
FROM devices
GROUP BY bucket, device_id
WITH NO DATA;

ALTER MATERIALIZED VIEW devices_hourly SET (timescaledb.enable_granular_refresh = true);

\set ON_ERROR_STOP 0
ALTER MATERIALIZED VIEW devices_daily SET (timescaledb.enable_granular_refresh = true);
\set ON_ERROR_STOP 1
:GRE 'devices_hourly';
:GRE 'devices_daily';

-- Dropping the first one frees the hypertable, so the second can enable it.
DROP MATERIALIZED VIEW devices_hourly;
ALTER MATERIALIZED VIEW devices_daily SET (timescaledb.enable_granular_refresh = true);
:GRE 'devices_daily';

DROP MATERIALIZED VIEW devices_daily;
DROP TABLE devices;

-- Error: timestamp and timestamptz not supported as granular refresh column types
CREATE TABLE events (time timestamptz NOT NULL, event_time timestamp, value float8);
SELECT create_hypertable('events', 'time', chunk_time_interval => '1 day'::interval);

\set ON_ERROR_STOP 0
ALTER TABLE events SET (
    timescaledb.granular_refresh_column = 'event_time',
    timescaledb.granular_refresh_start_offset = '2 months 30 days',
    timescaledb.granular_refresh_end_offset = '5 days'
);
\set ON_ERROR_STOP 1
:GRC 'events';

DROP TABLE events;

-- Error: character(n) wider than the tenant key limit.  char(n) blank-pads every
-- value to exactly n bytes regardless of the value's own length, so for n over
-- the 64 byte key limit no tenant is ever storable and tracking could never
-- engage.  Rejected up front rather than silently never working.
CREATE TABLE char_widths (time timestamptz NOT NULL, wide char(100), over char(65),
                          at_limit char(64), unpadded varchar(100), value float8);
SELECT create_hypertable('char_widths', 'time', chunk_time_interval => '1 day'::interval);

\set ON_ERROR_STOP 0
ALTER TABLE char_widths SET (
    timescaledb.granular_refresh_column = 'wide',
    timescaledb.granular_refresh_start_offset = '30 days',
    timescaledb.granular_refresh_end_offset = '1 day'
);
-- One byte over is still rejected.
ALTER TABLE char_widths SET (
    timescaledb.granular_refresh_column = 'over',
    timescaledb.granular_refresh_start_offset = '30 days',
    timescaledb.granular_refresh_end_offset = '1 day'
);
\set ON_ERROR_STOP 1
:GRC 'char_widths';

-- varchar(n) of any declared width is accepted: it does not pad, so a key is
-- only oversized when a value actually is, which the tracker handles by falling
-- back at insert time.
CREATE TABLE vc_width (time timestamptz NOT NULL, sensor varchar(100), value float8);
SELECT create_hypertable('vc_width', 'time', chunk_time_interval => '1 day'::interval);
ALTER TABLE vc_width SET (
    timescaledb.granular_refresh_column = 'sensor',
    timescaledb.granular_refresh_start_offset = '30 days',
    timescaledb.granular_refresh_end_offset = '1 day'
);
:GRC 'vc_width';

-- Exactly at the limit is accepted.
ALTER TABLE char_widths SET (
    timescaledb.granular_refresh_column = 'at_limit',
    timescaledb.granular_refresh_start_offset = '30 days',
    timescaledb.granular_refresh_end_offset = '1 day'
);
:GRC 'char_widths';

DROP TABLE char_widths;
DROP TABLE vc_width;
