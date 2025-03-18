#!/usr/bin/env bash
set -eu

#
# This script build a CHANGELOG.md for a new release
#

echo_changelog() {
    echo "${1}"
    # skip the template and release notes files
    grep -i "${2}" .unreleased/* | \
        cut -d: -f3- | sort | uniq | sed -e 's/^[[:space:]]*//' -e 's/^/* /' -e 's!#\([0-9]\+\)![#\1](https://github.com/timescale/timescaledb/pull/\1)!'
    echo
}

get_version_config_var() {
    grep "${1}" version.config | awk '{print $3}' | sed 's/-dev//'
}

RELEASE_NEXT=$(get_version_config_var '^version')
RELEASE_PREVIOUS=$(get_version_config_var '^update_from_version')
RELEASE_NOTES_HEADER_TEMPLATE="$(readlink -f $(dirname "${BASH_SOURCE[0]}"))/RELEASE_NOTES_HEADER.md.j2"

echo "Building CHANGELOG"
rm -f CHANGELOG_next.md
echo "## ${RELEASE_NEXT} ($(date +"%Y-%m-%d"))" > CHANGELOG_NEXT.md
echo "" >> CHANGELOG_NEXT.md
echo "This release contains performance improvements and bug fixes since " >> CHANGELOG_NEXT.md
echo "the 2.18.2 release. We recommend that you upgrade at the next " >> CHANGELOG_NEXT.md
echo "available opportunity." >> CHANGELOG_NEXT.md
echo "" >> CHANGELOG_NEXT.md
echo "**Highlighted features in TimescaleDB v${RELEASE_NEXT}**" >> CHANGELOG_NEXT.md
echo "* " >> CHANGELOG_NEXT.md
echo "" >> CHANGELOG_NEXT.md
echo_changelog '**Features**' '^Implements:' >> CHANGELOG_next.md
echo_changelog '**Bugfixes**' '^Fixes:' >> CHANGELOG_next.md
echo "**GUCs**" >> CHANGELOG_NEXT.md
echo "* " >> CHANGELOG_NEXT.md
echo "" >> CHANGELOG_NEXT.md
echo_changelog '**Thanks**' '^Thanks:' >> CHANGELOG_next.md

RELEASE_NOTE_START=$(grep -n $RELEASE_PREVIOUS CHANGELOG.md | cut -d ':' -f 1 | head -1)
CHANGELOG_HEADER_LINES=$((RELEASE_NOTE_START - 1))

mv CHANGELOG.md CHANGELOG.md.tmp
head -n $CHANGELOG_HEADER_LINES CHANGELOG.md.tmp > CHANGELOG.md
cat CHANGELOG_next.md >> CHANGELOG.md
CHANGELOG_LENGTH=$(wc -l CHANGELOG.md.tmp | cut -d ' ' -f 5)
CHANGELOG_ENTRIES=$((CHANGELOG_LENGTH-CHANGELOG_HEADER_LINES))
tail -n "$CHANGELOG_ENTRIES" CHANGELOG.md.tmp >> CHANGELOG.md
rm -f CHANGELOG.md.tmp CHANGELOG_next.md

# Remove the CHANGELOG generating
# Fresh start for next version
echo "Deleting all .unreleased files"
rm -f .unreleased/*

echo "done."
