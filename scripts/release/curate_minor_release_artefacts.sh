#!/bin/bash
set -eu

if [ "$#" -ne 2 ]; then
    echo "${0} <current_version> <next_version>"
    exit 2
fi

CURRENT_VERSION=$1
NEXT_VERSION=$2

UPDATE_FILE="$CURRENT_VERSION--$NEXT_VERSION.sql"
DOWNGRADE_FILE="$NEXT_VERSION--$CURRENT_VERSION.sql"
LAST_UPDATE_FILE=$(ls sql/updates/*--${CURRENT_VERSION}.sql | head -1 | cut -d '/' -f 3)
LAST_DOWNGRADE_FILE=$(ls sql/updates/${CURRENT_VERSION}--*.sql | head -1 | cut -d '/' -f 3)

# prepare next up & down files
echo "Generate upgrade and downgrade files"
cp ./sql/updates/latest-dev.sql ./sql/updates/$UPDATE_FILE
cp ./sql/updates/reverse-dev.sql ./sql/updates/$DOWNGRADE_FILE

truncate -s 0 ./sql/updates/latest-dev.sql
truncate -s 0 ./sql/updates/reverse-dev.sql

# CMakeLists
echo "Adding update & downgrade sql file to CMakeLists.txt"
gawk -i inplace '/'${LAST_UPDATE_FILE}')/ { print; print "    updates/'${UPDATE_FILE}')"; next }1' ./sql/CMakeLists.txt
sed -i.bak "s/${LAST_UPDATE_FILE})/${LAST_UPDATE_FILE}/g" CMakeLists.txt

gawk -i inplace '/ '${LAST_DOWNGRADE_FILE}')/ { print; print "    '${DOWNGRADE_FILE}')"; next }1' ./sql/CMakeLists.txt
sed -i.bak "s/ ${LAST_DOWNGRADE_FILE})/  ${LAST_DOWNGRADE_FILE}/g" CMakeLists.txt

sed -i.bak "s/FILE reverse-dev.sql)/FILE ${DOWNGRADE_FILE})/g" ./sql/CMakeLists.txt

# Remove the CHANGELOG generating
# Fresh start for next version
echo "Deleting all unreleased pr_* , fix_*"
rm -vf .unreleased/pr_* .unreleased/fix_*

# Set only next minor release version in version.config 
# and create this as a separate PR on `main`
echo "Set next minor release version.config"
sed -i.bak "s/${CURRENT_VERSION}/${NEXT_VERSION}/g" version.config
rm version.config.bak
head -n1 version.config
