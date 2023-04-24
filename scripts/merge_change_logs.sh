#!/bin/bash

cat << EOF
Please add the following to the changelog:

### Features ###
$(for file in .unreleased/features/*; do test -f "$file" && cat "$file"; done)

### Bug Fixes ###
$(for file in .unreleased/bugfixes/*; do test -f "$file" && cat "$file"; done)

### Thanks ###
$(for file in .unreleased/thanks/*; do test -f "$file" && cat "$file"; done)

EOF
