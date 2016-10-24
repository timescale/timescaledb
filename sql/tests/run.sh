#!/bin/bash
UPDATE_GOLDEN=true
j2 cluster.sql.j2 | psql -h localhost -U postgres -q -X > actual/cluster.out
diff actual/cluster.out expected/cluster.out
if [ $? -eq 0 ]
then
    echo "cluster matches golden file"
else
    if [ $UPDATE_GOLDEN = true ]
    then
        echo "updating cluster golden file"
        mv actual/cluster.out expected/cluster.out
    else
        echo "ERROR: golden file doesn't match: cluster.out"
    fi
fi