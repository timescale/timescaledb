## Using our Sample Datasets

### Available samples

We have created several sample datasets (using `pg_dump`) to help you get
started using TimescaleDB. These datasets vary in database size, number
of time intervals, and number of values for the partition field.

(Note that these dataset backups already include our time-series
  database, so you won't need to manually install our extension,
  nor run the setup scripts, etc.)

**Device ops**: These datasets are designed to represent metrics (e.g. CPU,
memory, network) collected from mobile devices.

1. [`devices_small`](https://timescaledata.blob.core.windows.net/datasets/devices_small.bak.tar.gz) - 1,000 devices recorded over 1,000 time intervals
1. [`devices_med`](https://timescaledata.blob.core.windows.net/datasets/devices_med.bak.tar.gz) - 5,000 devices recorded over 2,000 time intervals
1. [`devices_big`](https://timescaledata.blob.core.windows.net/datasets/devices_big.bak.tar.gz) - 3,000 devices recorded over 10,000 time intervals

For more details and example usage, see
[In-depth: Device ops datasets](#in-depth-devices).

**Weather**: These datasets are designed to represent temperature and
humidity data from a variety of locations.

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
2016-11-15 23:39:30-05 | demo004887 |                99.3
2016-11-15 23:39:30-05 | demo004882 |               100.8
2016-11-15 23:39:30-05 | demo004862 |                95.7
2016-11-15 23:39:30-05 | demo004844 |                95.5
2016-11-15 23:39:30-05 | demo004841 |                95.4
2016-11-15 23:39:30-05 | demo004804 |               101.6
2016-11-15 23:39:30-05 | demo004784 |               100.6
2016-11-15 23:39:30-05 | demo004760 |                99.1
2016-11-15 23:39:30-05 | demo004731 |                97.9
2016-11-15 23:39:30-05 | demo004729 |                99.6
(10 rows)```

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
2016-11-15 23:30:00-05 | demo003764 |        98.99 |            32 | discharging    | focus
2016-11-15 22:54:30-05 | demo001935 |        98.99 |            30 | discharging    | pinto
2016-11-15 19:10:30-05 | demo000695 |        98.99 |            23 | discharging    | focus
2016-11-15 16:46:00-05 | demo002784 |        98.99 |            18 | discharging    | pinto
2016-11-15 14:58:30-05 | demo004978 |        98.99 |            22 | discharging    | mustang
(5 rows)
```

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
2016-11-15 07:00:00-05 |                17 |                99
2016-11-15 08:00:00-05 |                11 |                98
2016-11-15 09:00:00-05 |                 6 |                97
2016-11-15 10:00:00-05 |                 6 |                97
2016-11-15 11:00:00-05 |                 6 |                97
2016-11-15 12:00:00-05 |                 6 |                97
2016-11-15 13:00:00-05 |                 6 |                97
2016-11-15 14:00:00-05 |                 6 |                98
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

time                   |     device_id      |    temperature     |      humidity
-----------------------+--------------------+--------------------+--------------------
2016-12-06 02:58:00-05 | weather-pro-000000 |  84.10000000000034 |  83.70000000000053
2016-12-06 02:58:00-05 | weather-pro-000001 | 35.999999999999915 |  51.79999999999994
2016-12-06 02:58:00-05 | weather-pro-000002 |  68.90000000000006 |  63.09999999999999
2016-12-06 02:58:00-05 | weather-pro-000003 |  83.70000000000041 |  84.69999999999989
2016-12-06 02:58:00-05 | weather-pro-000004 |  83.10000000000039 |  84.00000000000051
2016-12-06 02:58:00-05 | weather-pro-000005 |  85.10000000000034 |  81.70000000000017
2016-12-06 02:58:00-05 | weather-pro-000006 |  61.09999999999999 | 49.800000000000026
2016-12-06 02:58:00-05 | weather-pro-000007 |   82.9000000000004 |  84.80000000000047
2016-12-06 02:58:00-05 | weather-pro-000008 | 58.599999999999966 |               40.2
2016-12-06 02:58:00-05 | weather-pro-000009 | 61.000000000000014 | 49.399999999999906
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
2016-12-06 02:58:00-05 | weather-pro-000000 | field-000000  |       84.10 |    83.70
2016-12-06 02:58:00-05 | weather-pro-000001 | arctic-000000 |       35.99 |    51.79
2016-12-06 02:58:00-05 | weather-pro-000003 | swamp-000000  |       83.70 |    84.69
2016-12-06 02:58:00-05 | weather-pro-000004 | field-000001  |       83.10 |    84.00
2016-12-06 02:58:00-05 | weather-pro-000005 | swamp-000001  |       85.10 |    81.70
2016-12-06 02:58:00-05 | weather-pro-000007 | field-000002  |       82.90 |    84.80
2016-12-06 02:58:00-05 | weather-pro-000014 | field-000003  |       84.50 |    83.90
2016-12-06 02:58:00-05 | weather-pro-000015 | swamp-000002  |       85.50 |    66.00
2016-12-06 02:58:00-05 | weather-pro-000017 | arctic-000001 |       35.29 |    50.59
2016-12-06 02:58:00-05 | weather-pro-000019 | arctic-000002 |       36.09 |    48.80
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
2016-11-15 07:00:00-05 |    73.80 |    68.00 |    79.09
2016-11-15 08:00:00-05 |    74.80 |    68.69 |    80.29
2016-11-15 09:00:00-05 |    75.75 |    69.39 |    81.19
2016-11-15 10:00:00-05 |    76.75 |    70.09 |    82.29
2016-11-15 11:00:00-05 |    77.77 |    70.79 |    83.39
2016-11-15 12:00:00-05 |    78.76 |    71.69 |    84.49
2016-11-15 13:00:00-05 |    79.73 |    72.69 |    85.29
2016-11-15 14:00:00-05 |    80.72 |    73.49 |    86.99
2016-11-15 15:00:00-05 |    81.73 |    74.29 |    88.39
2016-11-15 16:00:00-05 |    82.70 |    75.09 |    88.89
2016-11-15 17:00:00-05 |    83.70 |    76.19 |    89.99
2016-11-15 18:00:00-05 |    84.67 |    77.09 |    90.00
2016-11-15 19:00:00-05 |    85.64 |    78.19 |    90.00
2016-11-15 20:00:00-05 |    86.53 |    78.59 |    90.00
2016-11-15 21:00:00-05 |    86.40 |    78.49 |    90.00
2016-11-15 22:00:00-05 |    85.39 |    77.29 |    89.30
2016-11-15 23:00:00-05 |    84.40 |    76.19 |    88.70
2016-11-16 00:00:00-05 |    83.39 |    75.39 |    87.90
2016-11-16 01:00:00-05 |    82.40 |    74.39 |    87.10
2016-11-16 02:00:00-05 |    81.40 |    73.29 |    86.29
2016-11-16 03:00:00-05 |    80.38 |    71.89 |    85.40
2016-11-16 04:00:00-05 |    79.41 |    70.59 |    84.40
2016-11-16 05:00:00-05 |    78.39 |    69.49 |    83.60
2016-11-16 06:00:00-05 |    78.42 |    69.49 |    84.40
(24 rows)
```
