# Testing using multiple versions 

This tests creates multiple instances with different upgrade
scenarios:

# CONTAINER_UPDATED:
- install previous version
- run test setup script
- update container
- ALTER EXTENSION UPDATE

# CONTAINER_CLEAN_RERUN:
- install main
- run test setup script

# CONTAINER_CLEAN_RESTORE:
- dump CONTAINER_UPDATED with pg_dump
- restore in new container

After those steps the test post script is run on all instances and
the output diffed, throwing an error if there is a difference between
any of the instances.

