#!/usr/bin/env bash
#
# This script runs pull-review on a pull request
# to intelligently assign reviewers to it.
#
PULL_REVIEW_GITHUB_TOKEN=$1
PULL_REQUEST_URL=$2

docker run ghcr.io/imsky/pull-review $PULL_REQUEST_URL --github-token $PULL_REVIEW_GITHUB_TOKEN

