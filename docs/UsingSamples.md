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
2016-11-15 23:39:30-05 | demo000000 |                 101
2016-11-15 23:39:30-05 | demo000016 |                  97
2016-11-15 23:39:30-05 | demo000019 |                96.3
2016-11-15 23:39:30-05 | demo000045 |                95.9
2016-11-15 23:39:30-05 | demo000048 |                97.1
2016-11-15 23:39:30-05 | demo000049 |               101.7
2016-11-15 23:39:30-05 | demo000070 |                95.9
2016-11-15 23:39:30-05 | demo000074 |                95.8
2016-11-15 23:39:30-05 | demo000089 |               100.5
2016-11-15 23:39:30-05 | demo000094 |               100.6
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
2016-11-15 22:51:00-05 | demo004320 |        98.99 |            32 | discharging    | pinto
2016-11-15 21:16:30-05 | demo001647 |        98.99 |            30 | discharging    | pinto
2016-11-15 19:13:00-05 | demo003758 |        98.99 |            30 | discharging    | focus
2016-11-15 17:09:00-05 | demo004924 |        98.99 |            15 | discharging    | mustang
2016-11-15 12:35:00-05 | demo002196 |        98.99 |            27 | discharging    | pinto
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
2016-11-15 07:00:00-05 |                19 |               100
2016-11-15 08:00:00-05 |                10 |               100
2016-11-15 09:00:00-05 |                 6 |               100
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

time                   |     device_id      |    temperature     |      humidity
-----------------------+--------------------+--------------------+--------------------
2016-12-06 02:58:00-05 | weather-pro-000000 | 33.899999999999885 | 52.799999999999955
2016-12-06 02:58:00-05 | weather-pro-000001 |  61.50000000000002 | 48.400000000000006
2016-12-06 02:58:00-05 | weather-pro-000002 | 34.599999999999895 | 49.399999999999906
2016-12-06 02:58:00-05 | weather-pro-000003 |  82.80000000000041 |  82.30000000000061
2016-12-06 02:58:00-05 | weather-pro-000004 |  85.00000000000034 |  76.00000000000013
2016-12-06 02:58:00-05 | weather-pro-000005 |               60.2 |  48.29999999999989
2016-12-06 02:58:00-05 | weather-pro-000006 |                 64 | 52.899999999999956
2016-12-06 02:58:00-05 | weather-pro-000007 |  83.10000000000039 |  83.00000000000057
2016-12-06 02:58:00-05 | weather-pro-000008 |  85.00000000000034 |  95.89999999999998
2016-12-06 02:58:00-05 | weather-pro-000009 |  83.30000000000044 |  92.19999999999995
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
2016-12-06 02:58:00-05 | weather-pro-000000 | arctic-000000 |       33.89 |    52.79
2016-12-06 02:58:00-05 | weather-pro-000002 | arctic-000001 |       34.59 |    49.39
2016-12-06 02:58:00-05 | weather-pro-000003 | field-000000  |       82.80 |    82.30
2016-12-06 02:58:00-05 | weather-pro-000004 | swamp-000000  |       85.00 |    76.00
2016-12-06 02:58:00-05 | weather-pro-000007 | field-000001  |       83.10 |    83.00
2016-12-06 02:58:00-05 | weather-pro-000008 | swamp-000001  |       85.00 |    95.89
2016-12-06 02:58:00-05 | weather-pro-000009 | swamp-000002  |       83.30 |    92.19
2016-12-06 02:58:00-05 | weather-pro-000011 | swamp-000003  |       85.20 |    82.79
2016-12-06 02:58:00-05 | weather-pro-000013 | arctic-000002 |       35.49 |    50.69
2016-12-06 02:58:00-05 | weather-pro-000018 | swamp-000004  |       83.40 |    60.60
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
2016-11-15 07:00:00-05 |    73.24 |    68.00 |    78.89
2016-11-15 08:00:00-05 |    74.25 |    69.09 |    80.09
2016-11-15 09:00:00-05 |    75.24 |    69.59 |    81.09
2016-11-15 10:00:00-05 |    76.25 |    70.69 |    82.39
2016-11-15 11:00:00-05 |    77.26 |    71.29 |    83.59
2016-11-15 12:00:00-05 |    78.26 |    72.39 |    84.79
2016-11-15 13:00:00-05 |    79.24 |    73.19 |    85.79
2016-11-15 14:00:00-05 |    80.22 |    73.69 |    86.79
2016-11-15 15:00:00-05 |    81.22 |    74.49 |    87.59
2016-11-15 16:00:00-05 |    82.24 |    75.29 |    88.59
2016-11-15 17:00:00-05 |    83.22 |    76.59 |    89.39
2016-11-15 18:00:00-05 |    84.23 |    77.59 |    90.00
2016-11-15 19:00:00-05 |    85.22 |    78.39 |    90.00
2016-11-15 20:00:00-05 |    85.16 |    78.29 |    90.00
2016-11-15 21:00:00-05 |    84.16 |    77.49 |    89.59
2016-11-15 22:00:00-05 |    83.16 |    76.19 |    88.59
2016-11-15 23:00:00-05 |    82.15 |    75.09 |    87.70
2016-11-16 00:00:00-05 |    81.15 |    74.09 |    86.70
2016-11-16 01:00:00-05 |    80.15 |    72.99 |    85.90
2016-11-16 02:00:00-05 |    79.14 |    71.89 |    84.80
2016-11-16 03:00:00-05 |    78.13 |    70.69 |    83.69
2016-11-16 04:00:00-05 |    77.15 |    69.39 |    82.80
2016-11-16 05:00:00-05 |    77.16 |    69.39 |    83.60
2016-11-16 06:00:00-05 |    78.13 |    70.59 |    84.90
(24 rows)
```
