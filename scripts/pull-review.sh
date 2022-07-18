#!/usr/bin/env bash
#
# This script runs pull-review on a pull request
# to intelligently assign reviewers to it.
#
PULL_REVIEW_GITHUB_TOKEN=$1
PULL_REQUEST_URL=$2

docker pull ghcr.io/imsky/pull-review

# this is what we should run eventually
# docker run ghcr.io/imsky/pull-review $PULL_REQUEST_URL --github-token $PULL_REVIEW_GITHUB_TOKEN

# check the output of the dry-run, before enabling reviewer assignment
docker run ghcr.io/imsky/pull-review $PULL_REQUEST_URL --dry-run --github-token $PULL_REVIEW_GITHUB_TOKEN > dry-run-output 2>&1
cat dry-run-output