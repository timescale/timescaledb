#!/bin/bash
set -e

UPDATE=${UPDATE:-false}

golden_test() {
	psql -h localhost -U postgres -q -X -f $1 2>&1 | tee actual/$2
	
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
mkdir -p dump
rm -fr dump/*


if [ "$#" -ne 0 ]; then
    tests="$@"
else
    tests=`ls -1 *.sql`
fi

for file in $tests
do
prefix="${file%%.*}"
echo "Running test '$prefix'"
golden_test ${prefix}.sql ${prefix}.out
done
echo "Success"
