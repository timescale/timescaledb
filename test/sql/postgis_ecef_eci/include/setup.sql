-- Common setup for PostGIS ECEF/ECI integration tests
-- Load the schema and functions before each test file

\ir ../../../../sql/postgis_ecef_eci/partitioning.sql
\ir ../../../../sql/postgis_ecef_eci/schema.sql
\ir ../../../../sql/postgis_ecef_eci/frame_conversion_stubs.sql
\ir ../../../../sql/postgis_ecef_eci/test_data_generator.sql
