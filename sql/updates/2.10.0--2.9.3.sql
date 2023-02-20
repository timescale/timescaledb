GRANT ALL ON _timescaledb_internal.job_errors TO PUBLIC;

ALTER EXTENSION timescaledb DROP VIEW timescaledb_information.job_errors;

DROP VIEW timescaledb_information.job_errors;
