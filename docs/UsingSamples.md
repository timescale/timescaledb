## Using our samples

### Available samples

We have created several sample datasets (using `pg_dump`) to help you get
started using iobeamdb. These datasets vary in database size, number of time
intervals, and number of values for the partition field.

**Device ops**: These datasets are designed to represent metrics (e.g. CPU,
memory, network) collected from mobile devices.
1. `dev_small` - 1,000 devices recorded over 1,000 time intervals
1. `dev_medium` - 10,000 devices recorded over 1,000 time intervals
1. `dev_big` - 3,000 devices recorded over 10,000 time intervals

(TODO - Host these somewhere and make linkable)

### Importing
Data is easily imported using the standard way of restoring `pg_dump` backups.
Briefly the steps are (1) unzip the archive, (2) create a database for the
data, and (3) import the data via `psql`. Each of our archives is named
`[dataset_name].bak.tar.gz`, so if you are using dataset `dev_small`, the
commands are:
```bash
# (1) unzip the archive
tar -xvzf dev_small.bak.tar.gz
# (2) create a database (e.g. called 'device_ops')
psql -U postgres -h localhost -c 'CREATE DATABASE device_ops;'
# (3) import data
psql -U postgres -d device_ops -h localhost < dev_small.bak
```

The data is now ready for you to use.
