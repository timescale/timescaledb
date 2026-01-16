-- TimescaleDB IoT Sensors Example Schema
--
-- This schema demonstrates optimal design for IoT device monitoring data.
-- It showcases TimescaleDB's hybrid row-columnar storage (columnstore) for
-- fast analytical queries while maintaining PostgreSQL compatibility.

-- Enable timing to show query performance
\timing on

-- Create the main sensor readings hypertable
-- This table will automatically partition data by time and enable columnstore
CREATE TABLE sensor_data (
    time TIMESTAMPTZ NOT NULL,
    device_id TEXT NOT NULL,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    pressure DOUBLE PRECISION,
    battery_level DOUBLE PRECISION,
    cpu_usage DOUBLE PRECISION,
    memory_usage DOUBLE PRECISION,
    location TEXT,
    status TEXT
) WITH (
    tsdb.hypertable,
    tsdb.partition_column='time',
    tsdb.enable_columnstore=true,
    tsdb.segmentby='device_id',
    tsdb.orderby='time DESC'
);

-- Create indexes for common query patterns
-- Index for device-specific queries
CREATE INDEX idx_sensor_device_time
    ON sensor_data (device_id, time DESC);

-- Index for location-based queries
CREATE INDEX idx_sensor_location_time
    ON sensor_data (location, time DESC)
    WHERE location IS NOT NULL;

-- Index for status-based queries (alerts, failures)
CREATE INDEX idx_sensor_status_time
    ON sensor_data (status, time DESC)
    WHERE status IS NOT NULL;

-- Add helpful table comments
COMMENT ON TABLE sensor_data IS 'IoT sensor readings with automatic time-based partitioning and columnstore compression';
COMMENT ON COLUMN sensor_data.time IS 'Timestamp of the sensor reading';
COMMENT ON COLUMN sensor_data.device_id IS 'Unique identifier for the IoT device';
COMMENT ON COLUMN sensor_data.temperature IS 'Temperature in degrees Celsius';
COMMENT ON COLUMN sensor_data.humidity IS 'Relative humidity percentage (0-100)';
COMMENT ON COLUMN sensor_data.pressure IS 'Atmospheric pressure in hPa';
COMMENT ON COLUMN sensor_data.battery_level IS 'Battery level percentage (0-100)';
COMMENT ON COLUMN sensor_data.cpu_usage IS 'CPU usage percentage (0-100)';
COMMENT ON COLUMN sensor_data.memory_usage IS 'Memory usage percentage (0-100)';
COMMENT ON COLUMN sensor_data.location IS 'Physical location of the device';
COMMENT ON COLUMN sensor_data.status IS 'Device status (normal, warning, error, offline)';

-- Display table information
\echo ''
\echo '=== Hypertable created successfully! ==='
\echo ''
\echo 'Table: sensor_data'
\echo 'Features enabled:'
\echo '  - Automatic time-based partitioning'
\echo '  - Columnstore compression for fast analytics'
\echo '  - Optimized segmentation by device_id'
\echo '  - Indexes for common query patterns'
\echo ''
\echo 'Next: Load sample data using iot-sample-data.csv'
\echo ''
