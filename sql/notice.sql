-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

DO language plpgsql $$
DECLARE
  telemetry_string TEXT;
  telemetry_level text;
BEGIN
  telemetry_level := current_setting('timescaledb.telemetry_level', true);
  CASE telemetry_level
  WHEN 'off' THEN
    telemetry_string = E'Note: Please enable telemetry to help us improve our product by running: ALTER DATABASE "' || current_database() || E'" SET timescaledb.telemetry_level = ''basic'';';
  WHEN 'basic' THEN
    telemetry_string = E'Note: TimescaleDB collects anonymous reports to better understand and assist our users.\nFor more information and how to disable, please see our docs https://docs.timescale.com/timescaledb/latest/how-to-guides/configuration/telemetry.';
  ELSE
    telemetry_string = E'';
  END CASE;

  RAISE WARNING E'%\n%\n',
    E'\nWELCOME TO\n' ||
    E' _   _       _     _____                      _____         _   _             \n' ||
    '| | | | ___ | |_  |  ___|__  _ __ __ _  ___  |_   _|__  ___| |_(_)_ __   __ _ ' ||E'\n' ||
$dyn$| |_| |/ _ \| __| | |_ / _ \| '__/ _` |/ _ \   | |/ _ \/ __| __| | '_ \ / _` |$dyn$ || E'\n' ||
    '|  _  | (_) | |_  |  _| (_) | | | (_| |  __/   | |  __/\__ \ |_| | | | | (_| |' ||E'\n' ||
    '|_| |_|\___/ \__| |_|  \___/|_|  \__, |\___|   |_|\___||___/\__|_|_| |_|\__, |' ||E'\n' ||
    '                                 |___/                                  |___/ ' ||E'\n' ||
    E'               Running version ' || '@PROJECT_VERSION_MOD@' || E'\n' ||

    E'For more information on TimescaleDB, please visit the following links:\n\n'
    ||
    E' 1. Getting started: https://docs.timescale.com/timescaledb/latest/getting-started\n' ||
    E' 2. API reference documentation: https://docs.timescale.com/api/latest\n' ||
    E' 3. How TimescaleDB is designed: https://docs.timescale.com/timescaledb/latest/overview/core-concepts\n',
    telemetry_string;
END;
$$;
