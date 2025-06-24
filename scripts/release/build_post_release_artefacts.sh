#!/bin/bash

#
# Build the necessary artefacts for the forwardporting the
# the release to `main` (Minor & Patch)
# - up & down files for the next version
# - latest.sql & reverse.sql
# - sets new version in version.config
# - adjusts the CMakeLists.txt
#
# Param: <published_version>
#

set -eu

if [ "$#" -ne 1 ]; then
    echo "${0} <published_version>"
    exit 2
fi

PUBLISHED_VERSION=$1
PREVIOUS_VERSION=$(tail -1 version.config | cut -d ' ' -f 3 | cut -d '-' -f 1)

echo "fetch the up & downgrade files"
URL_UPGRADE="https://raw.githubusercontent.com/timescale/timescaledb/refs/tags/${PUBLISHED_VERSION}/sql/updates/${PREVIOUS_VERSION}--${PUBLISHED_VERSION}.sql"
URL_DOWNGRADE="https://raw.githubusercontent.com/timescale/timescaledb/refs/tags/${PUBLISHED_VERSION}/sql/updates/${PUBLISHED_VERSION}--${PREVIOUS_VERSION}.sql"
echo $URL_UPGRADE
curl --fail -o sql/updates/${PREVIOUS_VERSION}--${PUBLISHED_VERSION}.sql $URL_UPGRADE
curl --fail -o sql/updates/${PUBLISHED_VERSION}--${PREVIOUS_VERSION}.sql $URL_DOWNGRADE

# find last up & down grade files
LAST_UPDATE_FILE=$(find sql/updates/*--${PREVIOUS_VERSION}.sql | head -1 | cut -d '/' -f 3)
LAST_DOWNGRADE_FILE=$(find sql/updates/${PREVIOUS_VERSION}--*.sql | head -1 | cut -d '/' -f 3)

echo "register upgrade sql file"
gawk -i inplace '/'${LAST_UPDATE_FILE}')/ { print; print "    updates/'${PREVIOUS_VERSION}'--'${PUBLISHED_VERSION}'.sql)"; next }1' ./sql/CMakeLists.txt
sed -i.bak "s/${LAST_UPDATE_FILE})/${LAST_UPDATE_FILE}/g" ./sql/CMakeLists.txt

echo "register downgrade sql file"
gawk -i inplace '/'${LAST_DOWNGRADE_FILE}')/ { print; print "    '${PUBLISHED_VERSION}'--'${PREVIOUS_VERSION}'.sql)"; next }1' ./sql/CMakeLists.txt
sed -i.bak "s/${LAST_DOWNGRADE_FILE})/${LAST_DOWNGRADE_FILE}/g" ./sql/CMakeLists.txt

# CHANGELOG
echo "fetching CHANGELOG"
URL_CHANGELOG="https://raw.githubusercontent.com/timescale/timescaledb/refs/tags/${PUBLISHED_VERSION}/CHANGELOG.md"
curl --silent --fail -o CHANGELOG.md $URL_CHANGELOG

# .unreleased files
echo "remove .unreleased files"
echo "TODO"

# Set new previous version
echo "Set new previous version version.config to ${PUBLISHED_VERSION}"
sed -i.bak "s/${PREVIOUS_VERSION}/${PUBLISHED_VERSION}/g" version.config
rm version.config.bak
tail -n 1 version.config
