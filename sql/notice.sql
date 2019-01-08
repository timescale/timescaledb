-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Apache License, see LICENSE-APACHE
-- at the top level directory of the TimescaleDB distribution.

DO language plpgsql $$
DECLARE
  end_time  TIMESTAMPTZ;
  expiration_time_string TEXT;
BEGIN
  end_time := _timescaledb_internal.license_expiration_time();

  IF end_time IS NOT NULL AND isfinite(end_time)
  THEN
    expiration_time_string = format(E'\nYour license expires on %s\n', end_time);
  ELSE
    expiration_time_string = '';
  END IF;

  RAISE WARNING E'%\n%',
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
    E' 1. Getting started: https://docs.timescale.com/getting-started\n' ||
    E' 2. API reference documentation: https://docs.timescale.com/api\n' ||
    E' 3. How TimescaleDB is designed: https://docs.timescale.com/introduction/architecture\n\n' ||
    E'Note: TimescaleDB collects anonymous reports to better understand and assist our users.\nFor more information and how to disable, please see our docs https://docs.timescaledb.com/using-timescaledb/telemetry.',
    expiration_time_string;

IF now() > end_time
THEN
  RAISE WARNING E'%\n', format('Your license expired on %s', end_time);
ELSIF now() + INTERVAL '1 week' >= end_time
THEN
  RAISE WARNING E'%\n', format('Your license will expires on %s', end_time);
END IF;

END;
$$;

select _timescaledb_internal.print_license_expiration_info();
