#!/usr/bin/env bash
#
# claude-analyze-failures.sh
#
# This script is invoked when nightly CI tests fail. It:
# 1. Downloads and aggregates test failure artifacts
# 2. Prepares context for Claude Code
# 3. Invokes Claude Code to analyze failures and propose fixes
# 4. Creates a separate PR for each fix with a descriptive title
#
# Required environment variables:
#   ANALYZE_RUN_ID     - The workflow run ID to analyze (preferred)
#                        Falls back to GITHUB_RUN_ID for backwards compatibility
#   SOURCE_REPOSITORY  - The source repository to clone and fetch artifacts from
#
# Optional environment variables (with defaults):
#   GITHUB_TOKEN       - GitHub token for target repo operations (default: from gh auth token)
#   SOURCE_GITHUB_TOKEN - GitHub token for source repo API calls (default: GITHUB_TOKEN)
#                         Required when source repo is in a different org than target
#
# Optional environment variables:
#   ANTHROPIC_API_KEY  - API key for Claude Code (not needed if logged in locally)
#   TARGET_REPOSITORY  - Repository to create the PR in (default: SOURCE_REPOSITORY)
#                        Useful for creating PRs in a fork during testing
#   BASE_BRANCH        - Branch to create the PR against (default: main)
#   CLAUDE_MODEL       - Model to use (default: claude-sonnet-4-20250514)
#   CLAUDE_BOT_USERNAME - Bot username (required, e.g., github-actions[bot])
#   MAX_ARTIFACTS      - Maximum number of artifacts to download (default: 10)
#   DRY_RUN            - If set to "true", skip Claude invocation and PR creation
#   SKIP_PR            - If set to "true", run Claude to make local changes but skip PR creation
#   KEEP_WORK_DIR      - If set to "true", keep the work directory in /tmp for inspection
#   ANALYSIS_OUTPUT_DIR - If set, copy Claude analysis output files to this directory before cleanup
#   SLACK_BOT_TOKEN    - Slack bot token for sending notifications (requires chat:write scope)
#   SLACK_CHANNEL      - Slack channel ID to post notifications to (e.g., C01234567)
#

set -euo pipefail

# Configuration
WORK_DIR="${WORK_DIR:-/tmp/claude-fix-$$}"
MAX_ARTIFACTS="${MAX_ARTIFACTS:-10}"
CLAUDE_MODEL="${CLAUDE_MODEL:-claude-sonnet-4-20250514}"
BASE_BRANCH="${BASE_BRANCH:-main}"
DRY_RUN="${DRY_RUN:-false}"
SKIP_PR="${SKIP_PR:-false}"
KEEP_WORK_DIR="${KEEP_WORK_DIR:-false}"
ANALYSIS_OUTPUT_DIR="${ANALYSIS_OUTPUT_DIR:-}"

# Set up debug logging early - capture all stderr to a log file
# Create work dir immediately so we can start logging
mkdir -p "${WORK_DIR}"
DEBUG_LOG="${WORK_DIR}/debug.log"
: > "${DEBUG_LOG}"  # Initialize empty log file

# Redirect stderr through tee to capture to log while still displaying
exec 2> >(tee -a "${DEBUG_LOG}" >&2)

# SOURCE_REPOSITORY is required (no default - must be explicitly set)
if [[ -z "${SOURCE_REPOSITORY:-}" ]]; then
    echo "ERROR: SOURCE_REPOSITORY is not set" >&2
    exit 1
fi
export SOURCE_REPOSITORY

# GITHUB_TOKEN defaults to gh auth token if not set
if [[ -z "${GITHUB_TOKEN:-}" ]]; then
    GITHUB_TOKEN=$(gh auth token 2>/dev/null || true)
fi
export GITHUB_TOKEN

# SOURCE_GITHUB_TOKEN is used for API calls to the source repository
# Defaults to GITHUB_TOKEN if not set (same-org scenario)
SOURCE_GITHUB_TOKEN="${SOURCE_GITHUB_TOKEN:-${GITHUB_TOKEN}}"
export SOURCE_GITHUB_TOKEN

# Debug: Check if we have separate tokens for source and target
if [[ "${SOURCE_GITHUB_TOKEN}" == "${GITHUB_TOKEN}" ]]; then
    echo "[DEBUG] Using same token for source and target repos" >&2
else
    echo "[DEBUG] Using separate token for source repo (cross-org)" >&2
fi

# ANALYZE_RUN_ID is the run ID to analyze. Fall back to GITHUB_RUN_ID for backwards
# compatibility, but note that GITHUB_RUN_ID is a built-in GitHub Actions variable
# that contains the CURRENT workflow's run ID, not the one we want to analyze.
# Using ANALYZE_RUN_ID avoids this conflict.
if [[ -z "${ANALYZE_RUN_ID:-}" ]]; then
    if [[ -n "${GITHUB_RUN_ID:-}" ]]; then
        ANALYZE_RUN_ID="${GITHUB_RUN_ID}"
        echo "[DEBUG] Using GITHUB_RUN_ID as fallback: ${ANALYZE_RUN_ID}" >&2
        echo "[WARN] GITHUB_RUN_ID may be the current workflow's run ID, not the target" >&2
    fi
else
    echo "[DEBUG] Using ANALYZE_RUN_ID: ${ANALYZE_RUN_ID}" >&2
fi
export ANALYZE_RUN_ID

# TARGET_REPOSITORY defaults to SOURCE_REPOSITORY (set in check_prerequisites)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $*" >&2
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $*" >&2
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*" >&2
}

cleanup() {
    # Save analysis output if requested
    if [[ -n "${ANALYSIS_OUTPUT_DIR}" && -d "${WORK_DIR}" ]]; then
        log_info "Saving analysis output to: ${ANALYSIS_OUTPUT_DIR}"
        mkdir -p "${ANALYSIS_OUTPUT_DIR}"

        # Copy the debug log (all stderr output)
        [[ -f "${DEBUG_LOG}" ]] && cp "${DEBUG_LOG}" "${ANALYSIS_OUTPUT_DIR}/"

        # Copy analysis output files (Claude's reasoning and conclusions)
        for f in "${WORK_DIR}"/analysis_*.txt; do
            [[ -f "$f" ]] && cp "$f" "${ANALYSIS_OUTPUT_DIR}/"
        done

        # Copy the failure context that was sent to Claude
        [[ -f "${WORK_DIR}/failure_context.md" ]] && cp "${WORK_DIR}/failure_context.md" "${ANALYSIS_OUTPUT_DIR}/"

        # Copy prompt files for debugging
        for f in "${WORK_DIR}"/prompt*.txt; do
            [[ -f "$f" ]] && cp "$f" "${ANALYSIS_OUTPUT_DIR}/"
        done

        # Copy any API error files for debugging
        for f in "${WORK_DIR}"/*_error.txt; do
            [[ -f "$f" ]] && cp "$f" "${ANALYSIS_OUTPUT_DIR}/"
        done

        # List what was saved
        log_info "Saved files:"
        ls -la "${ANALYSIS_OUTPUT_DIR}" >&2
    fi

    if [[ -d "${WORK_DIR}" ]]; then
        if [[ "${KEEP_WORK_DIR}" == "true" ]]; then
            log_info "Keeping work directory for inspection: ${WORK_DIR}"
        else
            log_info "Cleaning up work directory: ${WORK_DIR}"
            rm -rf "${WORK_DIR}"
        fi
    fi
}

trap cleanup EXIT

send_slack_notification() {
    local pr_count="$1"

    if [[ -z "${SLACK_BOT_TOKEN:-}" ]]; then
        log_info "SLACK_BOT_TOKEN not set, skipping Slack notification"
        return 0
    fi

    if [[ -z "${SLACK_CHANNEL:-}" ]]; then
        log_info "SLACK_CHANNEL not set, skipping Slack notification"
        return 0
    fi

    if [[ ! -s "${WORK_DIR}/created_prs.txt" ]]; then
        log_info "No PRs created, skipping Slack notification"
        return 0
    fi

    log_info "Sending Slack notification to channel ${SLACK_CHANNEL}..."

    # Build PR list for the message
    local pr_list=""
    local pr_buttons="["
    local first=true
    while read -r pr_url; do
        [[ -z "${pr_url}" ]] && continue
        pr_list="${pr_list}\n• <${pr_url}|${pr_url##*/}>"
        if [[ "${first}" == "true" ]]; then
            first=false
        else
            pr_buttons="${pr_buttons},"
        fi
        pr_buttons="${pr_buttons}{\"type\":\"button\",\"text\":{\"type\":\"plain_text\",\"text\":\"PR ${pr_url##*/}\"},\"url\":\"${pr_url}\"}"
    done < "${WORK_DIR}/created_prs.txt"
    pr_buttons="${pr_buttons}]"

    local message
    message=$(cat <<EOF
{
    "channel": "${SLACK_CHANNEL}",
    "text": "Claude created ${pr_count} PR(s) to fix nightly test failures",
    "blocks": [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": ":robot_face: *Claude created ${pr_count} PR(s) to fix nightly test failures*"
            }
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": "*PRs Created:*\n${pr_count}"
                },
                {
                    "type": "mrkdwn",
                    "text": "*Repository:*\n${TARGET_REPOSITORY:-${SOURCE_REPOSITORY}}"
                }
            ]
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*Pull Requests:*${pr_list}"
            }
        }
    ]
}
EOF
)

    local response
    response=$(curl -s -X POST \
        -H "Authorization: Bearer ${SLACK_BOT_TOKEN}" \
        -H 'Content-type: application/json; charset=utf-8' \
        --data "${message}" \
        "https://slack.com/api/chat.postMessage")

    if echo "${response}" | jq -e '.ok == true' > /dev/null 2>&1; then
        log_info "Slack notification sent successfully"
    else
        log_warn "Failed to send Slack notification: $(echo "${response}" | jq -r '.error // "unknown error"')"
    fi
}

check_prerequisites() {
    log_info "Checking prerequisites..."

    local missing_vars=()
    [[ -z "${GITHUB_TOKEN:-}" ]] && missing_vars+=("GITHUB_TOKEN (set it or run 'gh auth login')")
    [[ -z "${ANALYZE_RUN_ID:-}" ]] && missing_vars+=("ANALYZE_RUN_ID")
    [[ -z "${CLAUDE_BOT_USERNAME:-}" ]] && missing_vars+=("CLAUDE_BOT_USERNAME (e.g., github-actions[bot])")

    if [[ ${#missing_vars[@]} -gt 0 ]]; then
        log_error "Missing required environment variables: ${missing_vars[*]}"
        exit 1
    fi

    # Check for required tools
    for cmd in gh jq claude; do
        if ! command -v "$cmd" &>/dev/null; then
            log_error "Required command not found: $cmd"
            exit 1
        fi
    done

    # Check Claude authentication - either via API key or local login
    if [[ -z "${ANTHROPIC_API_KEY:-}" ]]; then
        log_info "ANTHROPIC_API_KEY not set, checking for local Claude authentication..."
        # Test if claude is authenticated by running a simple command
        if ! claude --version &>/dev/null; then
            log_error "Claude Code is not authenticated. Either set ANTHROPIC_API_KEY or run 'claude' to log in."
            exit 1
        fi
        log_info "Using local Claude authentication"
    else
        log_info "Using ANTHROPIC_API_KEY for authentication"
    fi

    # Set TARGET_REPOSITORY (defaults to SOURCE_REPOSITORY)
    TARGET_REPOSITORY="${TARGET_REPOSITORY:-${SOURCE_REPOSITORY}}"
    export TARGET_REPOSITORY
    if [[ "${TARGET_REPOSITORY}" != "${SOURCE_REPOSITORY}" ]]; then
        log_info "Artifacts from: ${SOURCE_REPOSITORY}"
        log_info "PR will be created in: ${TARGET_REPOSITORY}"
    fi

    log_info "All prerequisites satisfied"
}

get_failed_jobs() {
    log_info "Fetching failed jobs from run ${ANALYZE_RUN_ID}..."

    local jobs_file="${WORK_DIR}/failed_jobs.json"

    # Get all jobs from the run, filter for failed ones (excluding ignored failures)
    # Use SOURCE_GITHUB_TOKEN for cross-org access to source repository
    local api_error_file="${WORK_DIR}/api_error.txt"
    if ! GH_TOKEN="${SOURCE_GITHUB_TOKEN}" gh api "repos/${SOURCE_REPOSITORY}/actions/runs/${ANALYZE_RUN_ID}/jobs" \
        --paginate \
        --jq '.jobs[] | select(.conclusion == "failure") | {id: .id, name: .name}' \
        > "${jobs_file}" 2>"${api_error_file}"; then
        log_error "Failed to fetch jobs list from ${SOURCE_REPOSITORY}"
        log_error "API endpoint: repos/${SOURCE_REPOSITORY}/actions/runs/${ANALYZE_RUN_ID}/jobs"
        if [[ -s "${api_error_file}" ]]; then
            log_error "Error details:"
            cat "${api_error_file}" >&2
        fi
        log_error "This may be a permission issue - ensure the GitHub token has 'actions:read' access to ${SOURCE_REPOSITORY}"
        return 1
    fi

    if [[ ! -s "${jobs_file}" ]]; then
        log_warn "No failed jobs found"
        return 1
    fi

    local num_failed
    num_failed=$(wc -l < "${jobs_file}")
    log_info "Found ${num_failed} failed jobs"

    echo "${jobs_file}"
}

download_job_logs() {
    local jobs_file="$1"
    log_info "Downloading logs for failed jobs..."

    mkdir -p "${WORK_DIR}/logs"

    while IFS= read -r job; do
        [[ -z "${job}" ]] && continue

        local job_id job_name
        job_id=$(echo "$job" | jq -r '.id')
        job_name=$(echo "$job" | jq -r '.name')

        log_info "Downloading log for job: ${job_name}"
        local safe_name
        safe_name=$(echo "${job_name}" | tr '/' '_' | tr ' ' '_')

        if ! GH_TOKEN="${SOURCE_GITHUB_TOKEN}" gh api "repos/${SOURCE_REPOSITORY}/actions/jobs/${job_id}/logs" \
            > "${WORK_DIR}/logs/${safe_name}.log"; then
            log_warn "Failed to download log for job: ${job_name} (job_id: ${job_id})"
            continue
        fi
    done < "${jobs_file}"
}

extract_failed_tests() {
    local jobs_file="$1"
    log_info "Extracting failed tests from job logs..."

    local failed_tests_file="${WORK_DIR}/failed_tests.txt"
    : > "${failed_tests_file}"

    # Parse job logs for failed tests, excluding ignored ones
    for log_file in "${WORK_DIR}/logs"/*.log; do
        [[ -f "${log_file}" ]] || continue

        local job_name
        job_name=$(basename "${log_file}" .log)

        # Extract lines with "FAILED" or "not ok", excluding "(ignored)" entries
        grep -E "(FAILED|not ok)" "${log_file}" 2>/dev/null | \
            grep -v "(ignored)" | \
            while read -r line; do
                echo "${job_name}: ${line}" >> "${failed_tests_file}"
            done
    done

    if [[ -s "${failed_tests_file}" ]]; then
        local num_failed
        num_failed=$(wc -l < "${failed_tests_file}")
        log_info "Found ${num_failed} failed tests (excluding ignored)"
    else
        log_warn "No failed tests found in logs"
    fi

    echo "${failed_tests_file}"
}

download_failure_artifacts() {
    local jobs_file="$1"
    log_info "Downloading failure artifacts for failed jobs..."

    mkdir -p "${WORK_DIR}/artifacts"
    cd "${WORK_DIR}/artifacts"

    # Extract key identifiers from failed job names to match against artifacts
    # Job names are like: "PG17.2 Debug ubuntu-22.04"
    # Artifact names are like: "Regression diff ubuntu-22.04 Debug 17.2"
    # We extract: OS (ubuntu-22.04), build type (Debug/Release), PG version (17.2)
    local match_patterns_file="${WORK_DIR}/artifact_match_patterns.txt"
    : > "${match_patterns_file}"

    while IFS= read -r job; do
        [[ -z "${job}" ]] && continue
        local job_name
        job_name=$(echo "$job" | jq -r '.name')
        log_info "Failed job: ${job_name}"

        # Extract components from job name
        # Pattern: PG<version>[snapshot] <build_type> <os>
        local pg_version os_name
        pg_version=$(echo "${job_name}" | grep -oE 'PG[0-9]+(\.[0-9]+)*' | sed 's/^PG//')
        os_name=$(echo "${job_name}" | grep -oE '(ubuntu-[0-9.]+-*[a-z]*|macos-[0-9]+-*[a-z]*|timescaledb-runner-[a-z0-9]+)')

        # Build a pattern that matches artifacts for this job
        # Artifacts contain: os, build_type, and pg_version (without PG prefix)
        if [[ -n "${pg_version}" && -n "${os_name}" ]]; then
            # Escape dots in version for regex
            local escaped_version
            escaped_version="${pg_version//./\\.}"
            echo "${os_name}.*${escaped_version}|${escaped_version}.*${os_name}" >> "${match_patterns_file}"
            log_info "  -> Match pattern: ${os_name} + ${pg_version}"
        fi
    done < "${jobs_file}"

    if [[ ! -s "${match_patterns_file}" ]]; then
        log_warn "Could not extract match patterns from job names"
        return 1
    fi

    # Combine all patterns into one regex
    local combined_pattern
    combined_pattern=$(sort -u "${match_patterns_file}" | tr '\n' '|' | sed 's/|$//')
    log_info "Combined artifact match pattern: ${combined_pattern}"

    log_info "Fetching artifact list (with pagination)..."
    local artifacts_file="${WORK_DIR}/artifacts.json"

    # Get artifacts that match failed jobs and are relevant types
    # Use SOURCE_GITHUB_TOKEN for cross-org access to source repository
    local artifacts_error_file="${WORK_DIR}/artifacts_api_error.txt"
    if ! GH_TOKEN="${SOURCE_GITHUB_TOKEN}" gh api "repos/${SOURCE_REPOSITORY}/actions/runs/${ANALYZE_RUN_ID}/artifacts" \
        --paginate \
        --jq ".artifacts[] | select(.name | test(\"Regression|PostgreSQL log|Stacktrace|TAP\")) | {name: .name, id: .id}" \
        > "${artifacts_file}.all" 2>"${artifacts_error_file}"; then
        log_error "Failed to fetch artifacts list from ${SOURCE_REPOSITORY}"
        log_error "API endpoint: repos/${SOURCE_REPOSITORY}/actions/runs/${ANALYZE_RUN_ID}/artifacts"
        if [[ -s "${artifacts_error_file}" ]]; then
            log_error "Error details:"
            cat "${artifacts_error_file}" >&2
        fi
        log_error "This may be a permission issue - ensure the GitHub token has 'actions:read' access to ${SOURCE_REPOSITORY}"
        return 1
    fi

    # Filter artifacts matching our failed jobs
    : > "${artifacts_file}"
    while IFS= read -r artifact; do
        [[ -z "${artifact}" ]] && continue
        # Skip malformed JSON entries
        if ! echo "$artifact" | jq -e '.' >/dev/null 2>&1; then
            continue
        fi
        local name
        name=$(echo "$artifact" | jq -r '.name // empty')
        [[ -z "${name}" ]] && continue
        if echo "${name}" | grep -qE "${combined_pattern}"; then
            echo "${artifact}" >> "${artifacts_file}"
        fi
    done < "${artifacts_file}.all"

    if [[ ! -s "${artifacts_file}" ]]; then
        log_warn "No failure artifacts found for failed jobs"
        log_info "Available artifacts were:"
        jq -r '.name' "${artifacts_file}.all" | head -20 >&2
        return 1
    fi

    local total_artifacts
    total_artifacts=$(wc -l < "${artifacts_file}")
    log_info "Found ${total_artifacts} relevant artifacts for failed jobs"

    local count=0
    while IFS= read -r artifact; do
        [[ -z "${artifact}" ]] && continue

        if [[ ${count} -ge ${MAX_ARTIFACTS} ]]; then
            log_warn "Reached maximum artifact limit (${MAX_ARTIFACTS}), skipping remaining $((total_artifacts - count)) artifacts"
            break
        fi

        # Validate JSON before parsing
        if ! echo "$artifact" | jq -e '.' >/dev/null 2>&1; then
            log_warn "Skipping malformed artifact entry"
            continue
        fi

        local name id
        name=$(echo "$artifact" | jq -r '.name // empty')
        id=$(echo "$artifact" | jq -r '.id // empty')

        # Skip if name or id is empty
        if [[ -z "${name}" || -z "${id}" ]]; then
            log_warn "Skipping artifact with missing name or id"
            continue
        fi

        ((count++))
        log_info "Downloading artifact: ${name} (${count}/${total_artifacts})"
        GH_TOKEN="${SOURCE_GITHUB_TOKEN}" gh api "repos/${SOURCE_REPOSITORY}/actions/artifacts/${id}/zip" > "${name}.zip" || {
            log_warn "Failed to download artifact: ${name}"
            continue
        }

        mkdir -p "${name}"
        unzip -q "${name}.zip" -d "${name}" 2>/dev/null || {
            log_warn "Failed to unzip artifact: ${name}"
            continue
        }
        rm -f "${name}.zip"
    done < "${artifacts_file}"

    log_info "Downloaded ${count} artifacts"
    return 0
}

prepare_failure_context() {
    local failed_tests_file="$1"
    log_info "Preparing failure context for Claude..."

    local context_file="${WORK_DIR}/failure_context.md"

    cat > "${context_file}" << 'EOF'
# Test Failure Analysis

## Overview
The nightly CI tests have failed. Below is a summary of the failures.
Note: Tests marked as "(ignored)" have been excluded from this analysis.

EOF

    # Add failed jobs summary
    {
        echo "## Failed Jobs"
        echo ""
    } >> "${context_file}"

    if [[ -f "${WORK_DIR}/failed_jobs.json" ]]; then
        {
            echo '```'
            jq -r '.name' "${WORK_DIR}/failed_jobs.json"
            echo '```'
            echo ""
        } >> "${context_file}"
    fi

    # Add failed tests from job logs (excluding ignored)
    {
        echo "## Failed Tests (from job logs)"
        echo ""
    } >> "${context_file}"

    if [[ -f "${failed_tests_file}" && -s "${failed_tests_file}" ]]; then
        {
            echo '```'
            cat "${failed_tests_file}"
            echo '```'
        } >> "${context_file}"
    else
        echo "No specific failed tests identified in job logs." >> "${context_file}"
    fi
    echo "" >> "${context_file}"

    # Add regression diffs
    {
        echo "## Regression Diffs"
        echo ""
    } >> "${context_file}"

    find "${WORK_DIR}/artifacts" -name "regression.log" -o -name "*.diffs" 2>/dev/null | while read -r file; do
        if [[ -s "${file}" ]]; then
            {
                echo "### $(basename "$(dirname "${file}")")"
                echo '```diff'
                head -500 "${file}"
                if [[ $(wc -l < "${file}") -gt 500 ]]; then
                    echo ""
                    echo "... (truncated, ${file} has $(wc -l < "${file}") lines total)"
                fi
                echo '```'
                echo ""
            } >> "${context_file}"
        fi
    done

    # Add installcheck logs (failures only, excluding ignored)
    {
        echo "## Install Check Failures"
        echo ""
    } >> "${context_file}"

    find "${WORK_DIR}/artifacts" -name "installcheck.log" 2>/dev/null | while read -r file; do
        if [[ -s "${file}" ]]; then
            # Extract failed tests, excluding ignored ones
            local failures
            failures=$(grep -E "(FAILED|not ok)" "${file}" 2>/dev/null | grep -v "(ignored)" | head -100 || true)
            if [[ -n "${failures}" ]]; then
                {
                    echo "### $(basename "$(dirname "${file}")")"
                    echo '```'
                    echo "${failures}"
                    echo '```'
                    echo ""
                } >> "${context_file}"
            fi
        fi
    done

    # Add stack traces if present
    {
        echo "## Stack Traces"
        echo ""
    } >> "${context_file}"

    find "${WORK_DIR}/artifacts" -name "stacktrace.log" 2>/dev/null | while read -r file; do
        if [[ -s "${file}" ]]; then
            {
                echo "### $(basename "$(dirname "${file}")")"
                echo '```'
                head -200 "${file}"
                echo '```'
                echo ""
            } >> "${context_file}"
        fi
    done

    # Add PostgreSQL log excerpts (errors only)
    {
        echo "## PostgreSQL Errors"
        echo ""
    } >> "${context_file}"

    find "${WORK_DIR}/artifacts" -name "postmaster.*" 2>/dev/null | while read -r file; do
        if [[ -s "${file}" ]]; then
            local errors
            errors=$(grep -E "(ERROR|FATAL|PANIC)" "${file}" 2>/dev/null | head -50 || true)
            if [[ -n "${errors}" ]]; then
                {
                    echo "### $(basename "$(dirname "${file}")")"
                    echo '```'
                    echo "${errors}"
                    echo '```'
                    echo ""
                } >> "${context_file}"
            fi
        fi
    done

    log_info "Context prepared: ${context_file}"
    echo "${context_file}"
}

commit_claude_changes() {
    local test_name="$1"
    local analysis_output="$2"
    # Output variable for failure reason (caller can read COMMIT_FAILURE_REASON)
    COMMIT_FAILURE_REASON=""

    # Check if there are any changes to commit
    local modified_files new_files_list
    modified_files=$(git diff --name-only 2>/dev/null)
    new_files_list=$(git status --porcelain | grep '^??' | cut -c4- | while read -r file; do
        if ! grep -qxF "${file}" "${WORK_DIR}/untracked_before.txt" 2>/dev/null; then
            echo "${file}"
        fi
    done)

    if [[ -z "${modified_files}" && -z "${new_files_list}" ]]; then
        COMMIT_FAILURE_REASON="Claude analyzed the failure but did not modify any files"
        return 1
    fi

    # Filter out .github/ files - workflows require special scope, and scripts shouldn't be modified
    # This is a safety measure in case Claude modifies these files despite instructions
    local filtered_modified filtered_new
    filtered_modified=$(echo "${modified_files}" | grep -v '^\.github/' || true)
    filtered_new=$(echo "${new_files_list}" | grep -v '^\.github/' || true)

    # Check if any .github files were modified and warn
    local github_changes
    github_changes=$(echo "${modified_files}" | grep '^\.github/' || true)
    if [[ -n "${github_changes}" ]]; then
        log_warn "Skipping .github/ file changes:"
        echo "${github_changes}" | while read -r f; do
            log_warn "  - ${f}"
            git checkout -- "${f}" 2>/dev/null || true  # Revert the changes
        done >&2
    fi

    if [[ -z "${filtered_modified}" && -z "${filtered_new}" ]]; then
        COMMIT_FAILURE_REASON="Claude only modified .github/ files which are not allowed"
        return 1
    fi

    # Stage modified files (excluding .github/)
    if [[ -n "${filtered_modified}" ]]; then
        echo "${filtered_modified}" | xargs git add >&2
    fi

    # Stage new files created by Claude (excluding .github/)
    if [[ -n "${filtered_new}" ]]; then
        echo "${filtered_new}" | xargs git add >&2
    fi

    # Extract commit title from Claude's output using markers
    local title=""
    if grep -q "COMMIT_TITLE:" "${analysis_output}"; then
        title=$(sed -n '/COMMIT_TITLE:/,/END_COMMIT_TITLE/p' "${analysis_output}" | \
            grep -v "COMMIT_TITLE:\|END_COMMIT_TITLE\|COMMIT_MESSAGE:" | \
            head -1 | \
            sed 's/^[[:space:]]*//; s/[[:space:]]*$//' | \
            cut -c1-72)
    fi

    # Fallback title if extraction failed or result is invalid
    if [[ -z "${title}" ]] || [[ "${#title}" -gt 72 ]] || [[ "${title}" == *"COMMIT_MESSAGE"* ]]; then
        title="Fix test: ${test_name}"
    fi

    # Extract commit message body from Claude's output using markers
    local summary=""
    if grep -q "COMMIT_MESSAGE:" "${analysis_output}"; then
        summary=$(sed -n '/COMMIT_MESSAGE:/,/END_COMMIT_MESSAGE/p' "${analysis_output}" | \
            grep -v "COMMIT_MESSAGE:\|END_COMMIT_MESSAGE" | \
            sed 's/^[[:space:]]*//; s/[[:space:]]*$//' | \
            tr '\n' ' ' | \
            sed 's/[[:space:]]\+/ /g; s/^ //; s/ $//')
    fi

    # Fallback: if no markers found, use the last meaningful lines
    if [[ -z "${summary}" ]]; then
        summary=$(tail -20 "${analysis_output}" | grep -v '^[[:space:]]*$' | tail -5 | tr '\n' ' ' | \
            sed 's/[[:space:]]\+/ /g; s/^ //; s/ $//')
    fi

    # Create commit with descriptive message (redirect to stderr to avoid capturing in return value)
    git commit -m "$(cat <<EOF
${title}

${summary}

Co-Authored-By: Claude <noreply@anthropic.com>
EOF
)" >&2

    log_info "Committed fix for: ${test_name}"
    return 0
}

check_existing_pr_for_test() {
    local test_name="$1"

    # Search for open PRs with the claude-code label that mention this test
    # Check all authors to avoid duplicating work even if someone manually created a fix
    local matching_pr
    matching_pr=$(gh pr list \
        --repo "${TARGET_REPOSITORY}" \
        --state open \
        --label "claude-code" \
        --json number,title,url,body \
        --jq ".[] | select(.title + .body | test(\"${test_name}\"; \"i\")) | .url" \
        | head -1)

    if [[ -n "${matching_pr}" ]]; then
        echo "${matching_pr}"
        return 0
    fi

    return 1
}

fix_single_test() {
    local test_name="$1"
    local test_context="$2"
    local test_number="$3"
    local total_tests="$4"

    log_info "----------------------------------------------"
    log_info "FIXING TEST ${test_number}/${total_tests}: ${test_name}"
    log_info "----------------------------------------------"

    # Check if an unmerged PR already exists for this test
    local existing_pr
    if existing_pr=$(check_existing_pr_for_test "${test_name}"); then
        log_info "No fix applied for test: ${test_name}"
        log_info "Reason: An unmerged PR already exists: ${existing_pr}"
        # Create analysis file to document skip reason for artifact upload
        local analysis_output="${WORK_DIR}/analysis_${test_number}.txt"
        {
            echo "=============================================="
            echo "FIX NOT APPLIED - EXISTING PR"
            echo "=============================================="
            echo "Test: ${test_name}"
            echo "Reason: An unmerged PR already exists for this test"
            echo "Existing PR: ${existing_pr}"
            echo ""
            echo "Please review and merge the existing PR to resolve this failure."
        } > "${analysis_output}"
        return 2  # Return code 2 indicates "skipped due to existing PR"
    fi

    # Create a unique branch for this test fix
    local safe_test_name
    safe_test_name=$(echo "${test_name}" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]/-/g' | sed 's/--*/-/g' | sed 's/^-//; s/-$//' | cut -c1-40)
    local branch_name
    branch_name="claude-fix/${safe_test_name}-$(date +%Y%m%d-%H%M%S)"

    log_info "Creating branch: ${branch_name}"
    git checkout -b "${branch_name}" "${BASE_BRANCH}" >&2

    # Save list of untracked files before Claude runs
    git status --porcelain | grep '^??' | cut -c4- > "${WORK_DIR}/untracked_before.txt"

    # Create a prompt file for this specific test
    local prompt_file="${WORK_DIR}/prompt_${test_number}.txt"
    local analysis_output="${WORK_DIR}/analysis_${test_number}.txt"

    cat > "${prompt_file}" << EOF
I need you to fix a specific test failure from our nightly CI run.

## Test to Fix
Test name: ${test_name}

## Failure Context
${test_context}

## Instructions
1. Analyze why this specific test is failing
2. Identify the root cause
3. Implement a fix - either in the code or in the test expectations
4. Only fix actual bugs; don't just update test expectations unless the new behavior is correct

Important:
- Focus ONLY on fixing this one test: ${test_name}
- Do not modify files unrelated to this test
- NEVER modify files in .github/ (workflows, scripts, etc.)
- After making C code changes, run \`make format\` to format the code
- For other code (shell scripts, Python, etc.), check scripts/ for formatting tools
- NEVER modify expected output files (.out files) directly. Instead:
  1. Modify the test SQL file (.sql) if needed
  2. Run the test to generate new output
  3. If the test passes without errors, crashes, or truncated output, copy the new output over the expected file
  4. This ensures all output changes (including unexpected ones like plan changes) are captured
- Explain your reasoning briefly

After making changes, output BOTH a title and description for the commit in this exact format:

COMMIT_TITLE:
<A concise title describing the fix, ideally under 50 characters (max 72), e.g., "Fix race condition in chunk_column_stats">
END_COMMIT_TITLE

COMMIT_MESSAGE:
<A brief description of the root cause and the fix, suitable for a git commit message body. 2-4 sentences.>
END_COMMIT_MESSAGE
EOF

    local prompt_size
    prompt_size=$(wc -c < "${prompt_file}")
    log_info "Prompt size: ${prompt_size} bytes"

    local start_time
    start_time=$(date +%s)

    log_info "Starting Claude Code analysis for: ${test_name}"
    if ! claude -p - \
        --allowedTools "Edit,Write,Read,Glob,Grep,Bash" \
        < "${prompt_file}" \
        2>&1 | tee "${analysis_output}" >&2; then
        log_warn "No fix applied for test: ${test_name}"
        log_warn "Reason: Claude Code command exited with an error"
        # Append reason to analysis output for artifact upload
        {
            echo ""
            echo "=============================================="
            echo "FIX NOT APPLIED"
            echo "=============================================="
            echo "Reason: Claude Code command exited with an error"
        } >> "${analysis_output}"
        git checkout "${BASE_BRANCH}" >&2
        git branch -D "${branch_name}" >&2 2>/dev/null || true
        return 1
    fi

    local end_time duration
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    log_info "Analysis completed in ${duration} seconds"

    # Commit the changes for this test
    if ! commit_claude_changes "${test_name}" "${analysis_output}"; then
        log_warn "No fix applied for test: ${test_name}"
        if [[ -n "${COMMIT_FAILURE_REASON:-}" ]]; then
            log_warn "Reason: ${COMMIT_FAILURE_REASON}"
            # Append reason to analysis output for artifact upload
            {
                echo ""
                echo "=============================================="
                echo "FIX NOT APPLIED"
                echo "=============================================="
                echo "Reason: ${COMMIT_FAILURE_REASON}"
                echo ""
                echo "This may happen when Claude analyzes the failure and provides"
                echo "an explanation but does not make actual code changes. Review"
                echo "Claude's analysis above to understand the issue and determine"
                echo "if manual intervention is needed."
            } >> "${analysis_output}"
        fi
        git checkout "${BASE_BRANCH}" >&2
        git branch -D "${branch_name}" >&2 2>/dev/null || true
        return 1
    fi

    # Create PR for this specific fix (unless SKIP_PR is set)
    if [[ "${SKIP_PR}" != "true" ]]; then
        local pr_url
        if pr_url=$(create_pull_request "${branch_name}" "${test_name}" "${analysis_output}"); then
            log_info "PR created for ${test_name}: ${pr_url}"
            # Store PR URL for later notification
            echo "${pr_url}" >> "${WORK_DIR}/created_prs.txt"
        else
            log_warn "Failed to create PR for ${test_name}"
        fi
    fi

    # Return to base branch for next test
    git checkout "${BASE_BRANCH}" >&2

    return 0
}

extract_test_context() {
    local test_name="$1"
    local context_file="$2"

    # Extract relevant sections from the full context for this specific test
    local test_context=""

    # Look for this test in the regression diffs
    if [[ -d "${WORK_DIR}/artifacts" ]]; then
        # Find regression diff for this test
        local diff_file
        diff_file=$(find "${WORK_DIR}/artifacts" -name "regression.log" -o -name "*.diffs" 2>/dev/null | head -1)
        if [[ -f "${diff_file}" ]]; then
            # Extract section related to this test (if present)
            local test_diff
            test_diff=$(grep -A 100 "${test_name}" "${diff_file}" 2>/dev/null | head -100 || true)
            if [[ -n "${test_diff}" ]]; then
                test_context+="### Regression Diff
\`\`\`diff
${test_diff}
\`\`\`

"
            fi
        fi
    fi

    # Add the full context file as fallback
    test_context+="### Full Failure Context
$(cat "${context_file}")
"

    echo "${test_context}"
}

invoke_claude_code() {
    local context_file="$1"
    local failed_tests_file="$2"

    log_info "=============================================="
    log_info "INVOKING CLAUDE CODE"
    log_info "=============================================="

    cd "${CLONE_DIR}"

    # Initialize PR tracking file
    : > "${WORK_DIR}/created_prs.txt"

    # Get list of unique failed tests
    local unique_tests_file="${WORK_DIR}/unique_failed_tests.txt"
    if [[ -f "${failed_tests_file}" && -s "${failed_tests_file}" ]]; then
        log_info "Failed tests file contents:"
        head -20 "${failed_tests_file}" >&2

        # Extract just the test names (remove job prefix and duplicates)
        # Handle various formats:
        #   "test foo ... FAILED"
        #   "test foo ... FAILED (ignored)"  <- should be filtered already
        #   "not ok 1 - test_name"
        #   "not ok 51    + chunk_column_stats    1542 ms"  (TAP format with timestamp prefix)
        #   "foo ... FAILED 123 ms"
        sed 's/^[^:]*: //' "${failed_tests_file}" | \
            sed 's/^[^ ]* *//' | \
            tee "${WORK_DIR}/tests_without_prefix.txt" | \
            sed 's/^[[:space:]]*//' | \
            grep -oE 'test [^ ]+|^[^ ]+ +\.\.\.|not ok [0-9]+\s+[+-]\s+[a-zA-Z0-9_]+|not ok [0-9]+ - [^ ]+' | \
            sed 's/^test //; s/ *\.\.\..*//; s/^not ok [0-9]* - //; s/^not ok [0-9]*\s*[+-]\s*//' | \
            sort -u > "${unique_tests_file}"

        log_info "After extraction (tests_without_prefix.txt):"
        head -20 "${WORK_DIR}/tests_without_prefix.txt" >&2

        log_info "Unique tests extracted:"
        cat "${unique_tests_file}" >&2
    else
        log_warn "Failed tests file is empty or missing: ${failed_tests_file}"
    fi

    local total_tests=0
    local fixed_tests=0
    local failed_fixes=0
    local skipped_tests=0

    if [[ -f "${unique_tests_file}" && -s "${unique_tests_file}" ]]; then
        total_tests=$(wc -l < "${unique_tests_file}")
        log_info "Found ${total_tests} unique failed tests to fix"

        local test_number=0
        while IFS= read -r test_name; do
            [[ -z "${test_name}" ]] && continue
            ((test_number++))

            # Extract context specific to this test
            local test_context
            test_context=$(extract_test_context "${test_name}" "${context_file}")

            # Fix this specific test (creates its own branch and PR)
            local fix_result
            fix_single_test "${test_name}" "${test_context}" "${test_number}" "${total_tests}"
            fix_result=$?

            if [[ ${fix_result} -eq 0 ]]; then
                ((fixed_tests++))
            elif [[ ${fix_result} -eq 2 ]]; then
                ((skipped_tests++))
            else
                ((failed_fixes++))
            fi

        done < "${unique_tests_file}"
    else
        # Fallback: if we can't identify individual tests, do one big fix
        log_info "Could not identify individual tests, attempting bulk fix..."
        total_tests=1

        # Create a branch for the bulk fix
        local branch_name
        branch_name="claude-fix/nightly-$(date +%Y%m%d-%H%M%S)"
        log_info "Creating branch: ${branch_name}"
        git checkout -b "${branch_name}" "${BASE_BRANCH}" >&2

        # Save list of untracked files before Claude runs
        git status --porcelain | grep '^??' | cut -c4- > "${WORK_DIR}/untracked_before.txt"

        local prompt_file="${WORK_DIR}/prompt.txt"
        cat > "${prompt_file}" << EOF
I need you to analyze test failures from our nightly CI run and fix them.

Here is the failure context:
$(cat "${context_file}")

Please:
1. Analyze the test failures shown above
2. Identify the root cause of each failure
3. Implement fixes for the issues in the codebase
4. If the test expectations need updating (not the code), regenerate the expected output files

Important guidelines:
- Only fix actual bugs, don't just update test expectations to make tests pass unless the new behavior is correct
- If a test is flaky due to timing issues, make the test more robust rather than ignoring it
- NEVER modify files in .github/ (workflows, scripts, etc.)
- After making C code changes, run \`make format\` to format the code
- For other code (shell scripts, Python, etc.), check scripts/ for formatting tools
- NEVER modify expected output files (.out files) directly. Instead:
  1. Modify the test SQL file (.sql) if needed
  2. Run the test to generate new output
  3. If the test passes without errors, crashes, or truncated output, copy the new output over the expected file
  4. This ensures all output changes (including unexpected ones like plan changes) are captured
- Explain your reasoning for each fix

After making changes, output BOTH a title and description for the commit in this exact format:

COMMIT_TITLE:
<A concise title describing the fix, ideally under 50 characters (max 72)>
END_COMMIT_TITLE

COMMIT_MESSAGE:
<A brief description of the root cause and the fix, suitable for a git commit message body. 2-4 sentences.>
END_COMMIT_MESSAGE
EOF

        local analysis_output="${WORK_DIR}/analysis_output.txt"
        if claude -p - \
            --allowedTools "Edit,Write,Read,Glob,Grep,Bash" \
            < "${prompt_file}" \
            2>&1 | tee "${analysis_output}" >&2; then
            if commit_claude_changes "all-failures" "${analysis_output}"; then
                ((fixed_tests++))
                # Create PR for bulk fix
                if [[ "${SKIP_PR}" != "true" ]]; then
                    local pr_url
                    if pr_url=$(create_pull_request "${branch_name}" "all-failures" "${analysis_output}"); then
                        echo "${pr_url}" >> "${WORK_DIR}/created_prs.txt"
                    fi
                fi
            else
                log_warn "No fix applied for bulk analysis"
                if [[ -n "${COMMIT_FAILURE_REASON:-}" ]]; then
                    log_warn "Reason: ${COMMIT_FAILURE_REASON}"
                    # Append reason to analysis output for artifact upload
                    {
                        echo ""
                        echo "=============================================="
                        echo "FIX NOT APPLIED"
                        echo "=============================================="
                        echo "Reason: ${COMMIT_FAILURE_REASON}"
                    } >> "${analysis_output}"
                fi
                ((failed_fixes++))
            fi
        else
            log_warn "Claude Code command failed"
            # Append reason to analysis output for artifact upload
            {
                echo ""
                echo "=============================================="
                echo "FIX NOT APPLIED"
                echo "=============================================="
                echo "Reason: Claude Code command exited with an error"
            } >> "${analysis_output}"
            ((failed_fixes++))
        fi

        git checkout "${BASE_BRANCH}" >&2
    fi

    log_info "=============================================="
    log_info "SUMMARY"
    log_info "=============================================="
    log_info "Total tests attempted: ${total_tests}"
    log_info "Successfully fixed: ${fixed_tests}"
    log_info "Skipped (existing PR): ${skipped_tests}"
    log_info "Failed to fix: ${failed_fixes}"

    # Show created PRs
    if [[ -s "${WORK_DIR}/created_prs.txt" ]]; then
        log_info "PRs created:"
        while read -r pr_url; do
            log_info "  ${pr_url}"
        done < "${WORK_DIR}/created_prs.txt"
    fi

    log_info "=============================================="

    if [[ ${fixed_tests} -eq 0 ]]; then
        if [[ ${skipped_tests} -gt 0 ]]; then
            log_info "No new fixes needed - all failing tests have existing unmerged PRs"
            log_info "Please review and merge the existing PRs to resolve the failures"
        else
            log_error "No tests were fixed"
        fi
        return 1
    fi

    # Return the number of fixes (used for Slack notification)
    echo "${fixed_tests}"
}

create_pull_request() {
    local branch_name="$1"
    local test_name="${2:-}"
    local analysis_output="${3:-}"

    log_info "Creating pull request..."

    cd "${CLONE_DIR}"

    # Check if there are any commits on this branch
    local commit_count
    commit_count=$(git rev-list --count "${BASE_BRANCH}..HEAD" 2>/dev/null || echo "0")
    if [[ "${commit_count}" -eq 0 ]]; then
        log_warn "No commits were made by Claude Code"
        return 1
    fi

    log_info "Branch has ${commit_count} commit(s)"

    # Set up the remote for the target repository if different from source
    local push_remote="origin"
    if [[ "${TARGET_REPOSITORY}" != "${SOURCE_REPOSITORY}" ]]; then
        log_info "Setting up remote for target repository: ${TARGET_REPOSITORY}"
        # Use token in URL for authentication
        local target_url="https://x-access-token:${GITHUB_TOKEN}@github.com/${TARGET_REPOSITORY}.git"
        git remote add target "${target_url}" 2>/dev/null || \
            git remote set-url target "${target_url}"
        push_remote="target"
    else
        # For same repo, also ensure origin uses token authentication
        local origin_url="https://x-access-token:${GITHUB_TOKEN}@github.com/${SOURCE_REPOSITORY}.git"
        git remote set-url origin "${origin_url}" 2>/dev/null || true
    fi

    # Debug: show current branch and commits
    log_info "Current branch: $(git branch --show-current)"
    log_info "Commits to push (${BASE_BRANCH}..HEAD):"
    git log --oneline "${BASE_BRANCH}..HEAD" >&2

    # Verify we have commits
    local local_commit_count
    local_commit_count=$(git rev-list --count "${BASE_BRANCH}..HEAD" 2>/dev/null || echo "0")
    if [[ "${local_commit_count}" -eq 0 ]]; then
        log_error "No commits found between ${BASE_BRANCH} and HEAD"
        log_info "HEAD: $(git rev-parse HEAD)"
        log_info "${BASE_BRANCH}: $(git rev-parse "${BASE_BRANCH}")"
        return 1
    fi

    # Push the branch
    log_info "Pushing branch '${branch_name}' to remote '${push_remote}'..."
    if ! git push -u "${push_remote}" "${branch_name}" 2>&1 | tee "${WORK_DIR}/git_push_output.txt" >&2; then
        log_error "Failed to push branch. Output:"
        cat "${WORK_DIR}/git_push_output.txt" >&2
        return 1
    fi

    log_info "Push successful"

    # Generate PR title from commit message or test name
    local pr_title=""
    if [[ -n "${analysis_output}" ]] && [[ -f "${analysis_output}" ]]; then
        # Extract title from Claude's output
        if grep -q "COMMIT_TITLE:" "${analysis_output}"; then
            pr_title=$(sed -n '/COMMIT_TITLE:/,/END_COMMIT_TITLE/p' "${analysis_output}" | \
                grep -v "COMMIT_TITLE:\|END_COMMIT_TITLE\|COMMIT_MESSAGE:" | \
                head -1 | \
                sed 's/^[[:space:]]*//; s/[[:space:]]*$//')
        fi
    fi

    # Validate extracted title - reject if empty, too long, or contains markers
    if [[ -z "${pr_title}" ]] || [[ "${#pr_title}" -gt 256 ]] || [[ "${pr_title}" == *"COMMIT_MESSAGE"* ]]; then
        # Fallback: use commit subject line
        pr_title=$(git log -1 --format="%s" 2>/dev/null)
    fi

    # Final fallback: use test name
    if [[ -z "${pr_title}" ]] || [[ "${#pr_title}" -gt 256 ]] || [[ "${pr_title}" == *"COMMIT_MESSAGE"* ]]; then
        if [[ -n "${test_name}" ]]; then
            pr_title="Fix test: ${test_name}"
        else
            pr_title="Fix nightly test failure"
        fi
    fi

    # Generate commit details for PR body
    local commit_body
    commit_body=$(git log -1 --format="%b" 2>/dev/null)

    # Create the PR body
    local pr_body
    pr_body=$(cat <<EOF
## Summary

This PR was automatically generated by Claude Code to fix a failing nightly CI test.

### Test Fixed

\`${test_name}\`

### What Changed

${commit_body}

### Original Failure

- **Run ID**: ${ANALYZE_RUN_ID}
- **Run URL**: https://github.com/${SOURCE_REPOSITORY}/actions/runs/${ANALYZE_RUN_ID}

### Testing

Please review the fix and ensure it is appropriate before merging.
The CI will run the full test suite to verify the fix.

---

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)

    # Create the PR in the target repository
    log_info "Creating PR in repository: ${TARGET_REPOSITORY}"
    log_info "Base: ${BASE_BRANCH}, Head: ${branch_name}"

    # Save PR body to file for debugging (use safe_test_name to avoid path traversal)
    local safe_test_name_for_file
    safe_test_name_for_file=$(echo "${test_name}" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]/-/g' | sed 's/--*/-/g' | sed 's/^-//; s/-$//' | cut -c1-40)
    echo "${pr_body}" > "${WORK_DIR}/pr_body_${safe_test_name_for_file}.md"

    local pr_url
    local gh_output
    gh_output="${WORK_DIR}/gh_pr_output_${safe_test_name_for_file}.txt"

    # Determine head ref format
    local head_ref="${branch_name}"
    log_info "Creating PR: base=${BASE_BRANCH}, head=${head_ref}"
    log_info "PR title: ${pr_title}"

    if gh pr create \
        --repo "${TARGET_REPOSITORY}" \
        --title "${pr_title}" \
        --body "${pr_body}" \
        --base "${BASE_BRANCH}" \
        --head "${head_ref}" \
        2>&1 | tee "${gh_output}" >&2; then
        pr_url=$(grep -oE 'https://github.com/[^ ]+' "${gh_output}" | head -1)
        log_info "Pull request created: ${pr_url}"

        # Add label to identify this as a Claude-generated PR
        local label_name="claude-code"
        log_info "Adding label '${label_name}' to PR..."

        # Ensure the label exists (create if it doesn't)
        if ! gh label list --repo "${TARGET_REPOSITORY}" --search "${label_name}" | grep -q "${label_name}"; then
            log_info "Creating label '${label_name}'..."
            gh label create "${label_name}" \
                --repo "${TARGET_REPOSITORY}" \
                --description "PR generated by Claude Code" \
                --color "7C3AED" \
                &>/dev/null || log_warn "Could not create label (may already exist)"
        fi

        # Add the label to the PR
        gh pr edit "${head_ref}" \
            --repo "${TARGET_REPOSITORY}" \
            --add-label "${label_name}" \
            &>/dev/null || log_warn "Could not add label to PR"

        echo "${pr_url}"
    else
        log_error "Failed to create PR. Output:"
        cat "${gh_output}" >&2
        log_info "You can manually create a PR with:"
        log_info "  gh pr create --repo ${TARGET_REPOSITORY} --base ${BASE_BRANCH} --head ${head_ref}"
        return 1
    fi
}

main() {
    log_info "Starting Claude failure analysis..."
    log_info "Source repository: ${SOURCE_REPOSITORY:-not set}"
    log_info "Target repository: ${TARGET_REPOSITORY:-${SOURCE_REPOSITORY:-not set}}"
    log_info "Base branch: ${BASE_BRANCH}"
    log_info "Run ID: ${ANALYZE_RUN_ID:-not set}"
    log_info "Mode: $(if [[ "${DRY_RUN}" == "true" ]]; then echo "DRY_RUN"; elif [[ "${SKIP_PR}" == "true" ]]; then echo "SKIP_PR"; else echo "FULL"; fi)"

    check_prerequisites

    mkdir -p "${WORK_DIR}"

    # Clone repo to temp directory for isolation
    # When TARGET_REPOSITORY differs from SOURCE_REPOSITORY, clone from TARGET
    # to ensure we don't include files (like workflows) that exist in source but not target
    CLONE_DIR="${WORK_DIR}/repo"
    local clone_repo="${TARGET_REPOSITORY}"
    log_info "Cloning ${clone_repo} to temp directory..."
    if ! git clone --depth=1 --branch "${BASE_BRANCH}" \
        "https://x-access-token:${GITHUB_TOKEN}@github.com/${clone_repo}.git" \
        "${CLONE_DIR}" >&2; then
        log_error "Failed to clone repository"
        exit 1
    fi
    log_info "Clone complete"

    # Warn if source and target repos are at different commits
    # Note: This is informational only. The failed workflow may have run on a
    # different commit than either repo's current HEAD, so this check can't
    # definitively detect problems.
    if [[ "${TARGET_REPOSITORY}" != "${SOURCE_REPOSITORY}" ]]; then
        local target_sha source_sha
        target_sha=$(git -C "${CLONE_DIR}" rev-parse HEAD 2>/dev/null)
        source_sha=$(GH_TOKEN="${SOURCE_GITHUB_TOKEN}" gh api "repos/${SOURCE_REPOSITORY}/commits/${BASE_BRANCH}" --jq '.sha' || echo "")

        if [[ -n "${source_sha}" && "${target_sha}" != "${source_sha}" ]]; then
            log_warn "Source and target repos are at different commits:"
            log_warn "  Source (${SOURCE_REPOSITORY}): ${source_sha:0:12}"
            log_warn "  Target (${TARGET_REPOSITORY}): ${target_sha:0:12}"
        fi
    fi

    # Step 1: Get list of failed jobs
    local jobs_file
    jobs_file=$(get_failed_jobs)
    if [[ -z "${jobs_file}" || ! -s "${jobs_file}" ]]; then
        log_error "No failed jobs found in run ${ANALYZE_RUN_ID}"
        exit 1
    fi

    # Step 2: Download job logs and extract failed tests
    download_job_logs "${jobs_file}"
    local failed_tests_file
    failed_tests_file=$(extract_failed_tests "${jobs_file}")

    # Step 3: Download artifacts only for failed jobs
    if ! download_failure_artifacts "${jobs_file}"; then
        log_warn "Failed to download some failure artifacts, continuing with available data"
    fi

    # Step 4: Prepare context with failed tests info
    local context_file
    context_file=$(prepare_failure_context "${failed_tests_file}")

    if [[ "${DRY_RUN}" == "true" ]]; then
        log_info "Dry run mode - skipping Claude Code invocation and PR creation"
        log_info "Context file: ${context_file}"
        if [[ "${KEEP_WORK_DIR}" == "true" ]]; then
            log_info "Work directory contents:"
            ls -la "${WORK_DIR}"
        fi
        cat "${context_file}"
        exit 0
    fi

    # Step 5: Invoke Claude Code (creates separate PR for each fix)
    local fixed_count
    fixed_count=$(invoke_claude_code "${context_file}" "${failed_tests_file}")

    if [[ "${SKIP_PR}" == "true" ]]; then
        log_info "SKIP_PR mode - PRs were not created"
        log_info "Fixed ${fixed_count} tests locally"
        exit 0
    fi

    # Step 6: Send Slack notification for all created PRs
    send_slack_notification "${fixed_count}"

    # Show summary
    local pr_count=0
    if [[ -s "${WORK_DIR}/created_prs.txt" ]]; then
        pr_count=$(wc -l < "${WORK_DIR}/created_prs.txt")
    fi
    log_info "Done! Created ${pr_count} PR(s) for ${fixed_count} fix(es)"
}

main "$@"
