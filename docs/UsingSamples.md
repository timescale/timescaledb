## Using our Sample Datasets

### Available samples

We have created several sample datasets (using `pg_dump`) to help you get
started using TimescaleDB. These datasets vary in database size, number
of time intervals, and number of values for the partition field.

(Note that these dataset backups already include our time-series
database, so you won't need to manually install our extension,
nor run the setup scripts, etc.)

**Device ops**: These datasets are designed to represent metrics (e.g. CPU,
memory, network) collected from mobile devices. (Click on the name to
download.)

1. [`devices_small`](https://timescaledata.blob.core.windows.net/datasets/devices_small.bak.tar.gz) - 1,000 devices recorded over 1,000 time intervals
1. [`devices_med`](https://timescaledata.blob.core.windows.net/datasets/devices_med.bak.tar.gz) - 5,000 devices recorded over 2,000 time intervals
1. [`devices_big`](https://timescaledata.blob.core.windows.net/datasets/devices_big.bak.tar.gz) - 3,000 devices recorded over 10,000 time intervals

For more details and example usage, see
[In-depth: Device ops datasets](#in-depth-devices).

**Weather**: These datasets are designed to represent temperature and
humidity data from a variety of locations. (Click on the name to download.)

1. [`weather_small`](https://timescaledata.blob.core.windows.net/datasets/weather_small.bak.tar.gz) - 1,000 locations over 1,000 two-minute intervals
1. [`weather_med`](https://timescaledata.blob.core.windows.net/datasets/weather_med.bak.tar.gz) - 1,000 locations over 15,000 two-minute intervals
1. [`weather_big`](https://timescaledata.blob.core.windows.net/datasets/weather_big.bak.tar.gz) - 2,000 locations over 20,000 two-minute intervals

For more details and example usage, see
[In-depth: Weather datasets](#in-depth-weather).


### Importing
Data is easily imported using the standard way of restoring `pg_dump` backups.

Briefly the steps are:

1. Unzip the archive,
1. Create a database for the data (using the same name as the dataset)
1. Import the data via `psql`

Each of our archives is named `[dataset_name].bak.tar.gz`, so if you are using
dataset `devices_small`, the commands are:
```bash
# (1) unzip the archive
tar -xvzf devices_small.bak.tar.gz
# (2) create a database with the same name
psql -U postgres -h localhost -c 'CREATE DATABASE devices_small;'
# (3) import data
psql -U postgres -d devices_small -h localhost < devices_small.bak
```

The data is now ready for you to use.

```bash
# To access your database (e.g., devices_small)
psql -U postgres -h localhost -d devices_small
```

### In-depth: Device ops datasets <a name="in-depth-devices"></a>
After importing one of these datasets (`devices_small`, `devices_med`,
`devices_big`), you will find a plain Postgres table called `device_info`
and a hypertable called `readings`. The `device_info` table has (static)
metadata about each device, such as the OS name and manufacturer. The
`readings` hypertable tracks data sent from each device, e.g. CPU activity,
memory levels, etc. Because hypertables are exposed as a single table, you
can query them and join them with the metadata as you would normal SQL
tables (see Example Queries below).

#### Schemas
```sql
Table "public.device_info"
 Column      | Type | Modifiers
-------------+------+-----------
device_id    | text |
api_version  | text |
manufacturer | text |
model        | text |
os_name      | text |
```

```sql
Table "public.readings"
Column              |       Type       | Modifiers
--------------------+------------------+-----------
time                | bigint           |
device_id           | text             |
battery_level       | double precision |
battery_status      | text             |
battery_temperature | double precision |
bssid               | text             |
cpu_avg_1min        | double precision |
cpu_avg_5min        | double precision |
cpu_avg_15min       | double precision |
mem_free            | double precision |
mem_used            | double precision |
rssi                | double precision |
ssid                | text             |
Indexes:
"readings_device_id_time_idx" btree (device_id, "time" DESC)
"readings_time_idx" btree ("time" DESC)
```

#### Example Queries
_Note: Uses dataset_ `devices_med`

**10 most recent battery temperature readings for charging devices**
```sql
SELECT time, device_id, battery_temperature
FROM readings
WHERE battery_status = 'charging'
ORDER BY time DESC LIMIT 10;

time                   | device_id  | battery_temperature
-----------------------+------------+---------------------
2016-11-15 23:39:30-05 | demo004866 |               101.1
2016-11-15 23:39:30-05 | demo004848 |                  96
2016-11-15 23:39:30-05 | demo004836 |                97.2
2016-11-15 23:39:30-05 | demo004827 |                99.5
2016-11-15 23:39:30-05 | demo004792 |               101.7
2016-11-15 23:39:30-05 | demo004761 |                95.7
2016-11-15 23:39:30-05 | demo004740 |               100.2
2016-11-15 23:39:30-05 | demo004729 |                96.1
2016-11-15 23:39:30-05 | demo004723 |                95.4
2016-11-15 23:39:30-05 | demo004711 |                98.8
(10 rows)
```

**Busiest devices (1 min avg) whose battery level is below 33% and
is not charging**
```sql
SELECT time, readings.device_id, cpu_avg_1min,
battery_level, battery_status, device_info.model
FROM readings
JOIN device_info ON readings.device_id = device_info.device_id
WHERE battery_level < 33 AND battery_status = 'discharging'
ORDER BY cpu_avg_1min DESC, time DESC LIMIT 5;

time                   | device_id  | cpu_avg_1min | battery_level | battery_status |  model
-----------------------+------------+--------------+---------------+----------------+---------
2016-11-15 21:26:00-05 | demo000559 |        98.99 |            32 | discharging    | mustang
2016-11-15 20:31:00-05 | demo000312 |        98.99 |            29 | discharging    | pinto
2016-11-15 18:00:00-05 | demo002979 |        98.99 |            26 | discharging    | pinto
2016-11-15 17:40:00-05 | demo003978 |        98.99 |            25 | discharging    | pinto
2016-11-15 13:55:30-05 | demo004548 |        98.99 |            12 | discharging    | pinto
(5 rows)
```

**Minimum and maximum battery levels per hour for the first 12 hours**
```sql
SELECT date_trunc('hour', time) "hour",
min(battery_level) min_battery_level,
max(battery_level) max_battery_level
FROM readings r
WHERE r.device_id IN (
    SELECT DISTINCT device_id FROM device_info
    WHERE model = 'pinto' OR model = 'focus'
) GROUP BY "hour" ORDER BY "hour" ASC LIMIT 12;

hour                   | min_battery_level | max_battery_level
-----------------------+-------------------+-------------------
2016-11-15 07:00:00-05 |                20 |                99
2016-11-15 08:00:00-05 |                12 |                98
2016-11-15 09:00:00-05 |                 8 |                97
2016-11-15 10:00:00-05 |                 6 |               100
2016-11-15 11:00:00-05 |                 6 |               100
2016-11-15 12:00:00-05 |                 6 |               100
2016-11-15 13:00:00-05 |                 6 |               100
2016-11-15 14:00:00-05 |                 6 |               100
2016-11-15 15:00:00-05 |                 6 |               100
2016-11-15 16:00:00-05 |                 6 |               100
2016-11-15 17:00:00-05 |                 6 |               100
2016-11-15 18:00:00-05 |                 6 |               100
(12 rows)
```

**

### In-depth: Weather datasets <a name="in-depth-weather"></a>
After importing one of these datasets (`weather_small`, `weather_med`,
`weather_big`), you will find a plain Postgres table called `locations` and
a hypertable called `conditions`. The `locations` table has metadata about
each of the locations, such as its name and environmental type. The
`conditions` hypertable tracks readings of temperature and humidity from
those locations. Because hypertables are exposed as a single table, you can
query them and join them with the metadata as you would normal SQL tables (see Example Queries below).

#### Schemas
```sql
Table "public.locations"
Column      | Type | Modifiers
------------+------+-----------
device_id   | text |
location    | text |
environment | text |
```
```sql
Table "public.conditions"
Column      |           Type           | Modifiers
------------+--------------------------+-----------
time        | timestamp with time zone | not null
device_id   | text                     |
temperature | double precision         |
humidity    | double precision         |
Indexes:
"conditions_device_id_time_idx" btree (device_id, "time" DESC)
"conditions_time_idx" btree ("time" DESC)
```

#### Example Queries
_Note: Uses dataset_ `weather_med`

**Last 10 readings**
```sql
SELECT * FROM conditions c ORDER BY time DESC LIMIT 10;

time                   |     device_id      |    temperature    |     humidity
-----------------------+--------------------+-------------------+-------------------
2016-12-06 02:58:00-05 | weather-pro-000999 | 35.49999999999991 | 50.60000000000004
2016-12-06 02:58:00-05 | weather-pro-000998 |  34.7999999999999 | 48.70000000000001
2016-12-06 02:58:00-05 | weather-pro-000997 | 84.90000000000035 | 86.30000000000001
2016-12-06 02:58:00-05 | weather-pro-000996 | 83.40000000000038 | 83.40000000000055
2016-12-06 02:58:00-05 | weather-pro-000995 | 83.30000000000038 |  82.4000000000006
2016-12-06 02:58:00-05 | weather-pro-000994 |  83.9000000000004 | 70.70000000000024
2016-12-06 02:58:00-05 | weather-pro-000993 | 35.79999999999991 | 51.30000000000005
2016-12-06 02:58:00-05 | weather-pro-000992 | 81.90000000000046 |  82.4000000000006
2016-12-06 02:58:00-05 | weather-pro-000991 |              62.1 |              48.2
2016-12-06 02:58:00-05 | weather-pro-000990 | 83.60000000000042 | 76.70000000000014
(10 rows)
```
**Last 10 readings from 'outside' locations**
```sql
SELECT time, c.device_id, location,
trunc(temperature, 2) temperature, trunc(humidity, 2) humidity
FROM conditions c
INNER JOIN locations l ON c.device_id = l.device_id
WHERE l.environment = 'outside'
ORDER BY time DESC LIMIT 10;

time                   |     device_id      |   location    | temperature | humidity
-----------------------+--------------------+---------------+-------------+----------
2016-12-06 02:58:00-05 | weather-pro-000999 | arctic-000204 |       35.49 |    50.60
2016-12-06 02:58:00-05 | weather-pro-000998 | arctic-000203 |       34.79 |    48.70
2016-12-06 02:58:00-05 | weather-pro-000997 | swamp-000213  |       84.90 |    86.30
2016-12-06 02:58:00-05 | weather-pro-000996 | field-000199  |       83.40 |    83.40
2016-12-06 02:58:00-05 | weather-pro-000995 | field-000198  |       83.30 |    82.40
2016-12-06 02:58:00-05 | weather-pro-000994 | swamp-000212  |       83.90 |    70.70
2016-12-06 02:58:00-05 | weather-pro-000993 | arctic-000202 |       35.79 |    51.30
2016-12-06 02:58:00-05 | weather-pro-000992 | field-000197  |       81.90 |    82.40
2016-12-06 02:58:00-05 | weather-pro-000990 | swamp-000211  |       83.60 |    76.70
2016-12-06 02:58:00-05 | weather-pro-000989 | field-000196  |       83.30 |    81.70
(10 rows)
```

**Hourly average, min, and max temperatures for "field" locations**
```sql
SELECT date_trunc('hour', time) "hour",
trunc(avg(temperature), 2) avg_temp,
trunc(min(temperature), 2) min_temp,
trunc(max(temperature), 2) max_temp
FROM conditions c
WHERE c.device_id IN (
    SELECT device_id FROM locations
    WHERE location LIKE 'field-%'
) GROUP BY "hour" ORDER BY "hour" ASC LIMIT 24;

hour                   | avg_temp | min_temp | max_temp
-----------------------+----------+----------+----------
2016-11-15 07:00:00-05 |    73.59 |    68.10 |    79.09
2016-11-15 08:00:00-05 |    74.59 |    68.89 |    80.19
2016-11-15 09:00:00-05 |    75.60 |    69.79 |    81.69
2016-11-15 10:00:00-05 |    76.59 |    70.49 |    82.79
2016-11-15 11:00:00-05 |    77.59 |    71.69 |    84.19
2016-11-15 12:00:00-05 |    78.59 |    72.79 |    85.49
2016-11-15 13:00:00-05 |    79.62 |    73.69 |    86.49
2016-11-15 14:00:00-05 |    80.62 |    74.29 |    87.49
2016-11-15 15:00:00-05 |    81.62 |    75.09 |    88.59
2016-11-15 16:00:00-05 |    82.63 |    76.09 |    89.89
2016-11-15 17:00:00-05 |    83.64 |    77.19 |    90.00
2016-11-15 18:00:00-05 |    84.63 |    77.99 |    90.00
2016-11-15 19:00:00-05 |    85.58 |    79.09 |    90.00
2016-11-15 20:00:00-05 |    85.48 |    79.19 |    90.00
2016-11-15 21:00:00-05 |    84.47 |    78.29 |    89.50
2016-11-15 22:00:00-05 |    83.48 |    77.09 |    88.49
2016-11-15 23:00:00-05 |    82.46 |    75.79 |    87.70
2016-11-16 00:00:00-05 |    81.45 |    75.09 |    86.80
2016-11-16 01:00:00-05 |    80.45 |    73.89 |    85.70
2016-11-16 02:00:00-05 |    79.45 |    72.79 |    84.90
2016-11-16 03:00:00-05 |    78.46 |    71.99 |    84.00
2016-11-16 04:00:00-05 |    77.48 |    71.09 |    83.00
2016-11-16 05:00:00-05 |    77.50 |    71.09 |    82.90
2016-11-16 06:00:00-05 |    78.49 |    71.69 |    84.20
(24 rows)
```
