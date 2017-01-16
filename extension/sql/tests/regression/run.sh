#!/bin/bash
set -e

UPDATE=${UPDATE:-false}

golden_test() {
	psql -h localhost -U postgres -q -X -f $1 > actual/$2
	
    if diff expected/$2 actual/$2;
	then
    	echo "$2 matches golden file"
	else
    	if [ $UPDATE = true ]
    	then
        	echo "updating $2 golden file"
        	mv actual/$2 expected/$2
    	else
        	echo "ERROR: golden file doesn't match: $2"
          	exit 1
    	fi
	fi
}

mkdir -p actual
rm -fr actual/*
golden_test cluster.sql cluster.out
golden_test kafka.sql kafka.out
golden_test insert.sql insert.out
golden_test ioql_query.sql ioql_query.out
golden_test sql_query.sql sql_query.out
golden_test ddl.sql ddl.out
golden_test timestamp.sql timestamp.out
golden_test drop_hypertable.sql drop_hypertable.out

echo "Success"
