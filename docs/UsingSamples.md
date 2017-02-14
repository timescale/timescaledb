## Using our Sample Datasets

### Available samples

We have created several sample datasets (using `pg_dump`) to help you get
started using iobeamdb. These datasets vary in database size, number of time
intervals, and number of values for the partition field.

(Note that these dataset backups already include our time-series
  database, so you won't need to manually install our extension,
  nor run the setup scripts, etc.)

**Device ops**: These datasets are designed to represent metrics (e.g. CPU,
memory, network) collected from mobile devices.
1. [`devices_small`](https://iobeamdata.blob.core.windows.net/datasets/devices_small.bak.tar.gz) - 1,000 devices recorded over 1,000 time intervals
1. [`devices_med`](https://iobeamdata.blob.core.windows.net/datasets/devices_med.bak.tar.gz) - 10,000 devices recorded over 1,000 time intervals
1. [`devices_big`](https://iobeamdata.blob.core.windows.net/datasets/devices_big.bak.tar.gz) - 3,000 devices recorded over 10,000 time intervals

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
