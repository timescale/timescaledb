#!/bin/bash
set -e

UPDATE=${UPDATE:-false}

golden_test() {
	psql -h localhost -U postgres -q -X -f $1 > actual/$2
	
  if diff actual/$2 expected/$2;
	then
    	echo "$2 matches golden file"
	else
    	if [ $UPDATE = true ]
    	then
        	echo "updating $2 golden file"
        	mv actual/$2 expected/$2
    	else
        	echo "ERROR: golden file doesn't match: $2"
    	fi
	fi
}

mkdir -p actual
rm -fr actual/*
golden_test cluster.sql cluster.out
golden_test kafka.sql kafka.out
golden_test insert.sql insert.out
golden_test query.sql query.out
echo "Success"
