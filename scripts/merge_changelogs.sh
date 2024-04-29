#!/usr/bin/env bash

#
# This script build a CHANGELOG.md entry for a new release
#

RELEASE_NOTES_TEMPLATE='.unreleased/RELEASE_NOTES.md.j2'

echo_changelog() {
    echo "${1}"
    # skip the template and release notes files
    grep -i "${2}" .unreleased/* | \
        grep -v '.unreleased/template.*' | \
        grep -v "${RELEASE_NOTES_TEMPLATE}" | \
        cut -d: -f3- | sort | uniq | sed -e 's/^[[:space:]]*//' -e 's/^/* /'
    echo
}

get_version_config_var() {
    grep "${1}" version.config | awk '{print $3}' | sed 's/-dev//'
}

RELEASE_CURRENT=$(get_version_config_var '^version')
RELEASE_PREVIOUS=$(get_version_config_var '^update_from_version')
RELEASE_DATE=$(date +"%Y-%m-%d")

#
# To install jinja template client:
#   $ pip install jinja-cli
#
if [ -f "${RELEASE_NOTES_TEMPLATE}" ];
then
    jinja \
        -D release_current "${RELEASE_CURRENT}" \
        -D release_previous "${RELEASE_PREVIOUS}" \
        -D release_date "${RELEASE_DATE}" ${RELEASE_NOTES_TEMPLATE}
    echo
fi

echo_changelog '**Features**' '^Implements:'
echo_changelog '**Bugfixes**' '^Fixes:'
echo_changelog '**Thanks**' '^Thanks:'
