#!/bin/bash

# skip the template file
echo "**Features**"
grep -i '^Implements:' .unreleased/* | grep -v '.unreleased/template.rfc822' | cut -d: -f3- | sort | uniq | sed -e 's/^[[:space:]]*//' -e 's/^/* /'

echo "**Bugfixes**"
grep -i '^Fixes:' .unreleased/* | grep -v '.unreleased/template.rfc822' | cut -d: -f3- | sort | uniq | sed -e 's/^[[:space:]]*//' -e 's/^/* /'

echo "**Thanks**"
grep -i '^Thanks:' .unreleased/* | grep -v '.unreleased/template.rfc822' | cut -d: -f3- | sort | sed -e 's/^[[:space:]]*//' -e 's/^/* /'
