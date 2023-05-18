#!/bin/bash

# Check if both old_version and new_version have been provided
if [[ -z "$1" || -z "$2" ]]; then
  echo "Usage: $0 <old_version> <new_version>"
  exit 1
fi

# Assign the values to variables for easier use later
OLD_VERSION="$1"
NEW_VERSION="$2"

# Run git diff and remove the listed files
git diff --name-only "$OLD_VERSION" "$NEW_VERSION" .unreleased | grep -v '.unreleased/template.rfc822' | xargs rm
