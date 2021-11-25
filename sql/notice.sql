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
    E' _____ _                               _     ____________  \n' ||
    E'|_   _(_)                             | |    |  _  \\ ___ \\ \n' ||
    E'  | |  _ _ __ ___   ___  ___  ___ __ _| | ___| | | | |_/ / \n' ||
    '  | | | |  _ ` _ \ / _ \/ __|/ __/ _` | |/ _ \ | | | ___ \ ' || E'\n' ||
    '  | | | | | | | | |  __/\__ \ (_| (_| | |  __/ |/ /| |_/ /' || E'\n' ||
    '  |_| |_|_| |_| |_|\___||___/\___\__,_|_|\___|___/ \____/' || E'\n' ||
    E'               Running version ' || '@PROJECT_VERSION_MOD@' || E'\n' ||

    E'For more information on TimescaleDB, please visit the following links:\n\n'
    ||
    E' 1. Getting started: https://docs.timescale.com/timescaledb/latest/getting-started\n' ||
    E' 2. API reference documentation: https://docs.timescale.com/api/latest\n' ||
    E' 3. How TimescaleDB is designed: https://docs.timescale.com/timescaledb/latest/overview/core-concepts\n',
    telemetry_string;
END;
$$;
