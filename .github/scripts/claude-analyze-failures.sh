#!/usr/bin/env bash
#
# claude-analyze-failures.sh
#
# This script is invoked when nightly CI tests fail. It:
# 1. Downloads and aggregates test failure artifacts
# 2. Prepares context for Claude Code
# 3. Invokes Claude Code to analyze failures and propose fixes
# 4. Creates a PR with the proposed fix
#
# Required environment variables:
#   GITHUB_RUN_ID      - The workflow run ID
#
# Optional environment variables (with defaults):
#   GITHUB_TOKEN       - GitHub token (default: from gh auth token)
#   GITHUB_REPOSITORY  - The source repository for artifacts (default: timescale/timescaledb)
#
# Optional environment variables:
#   ANTHROPIC_API_KEY  - API key for Claude Code (not needed if logged in locally)
#   TARGET_REPOSITORY  - Repository to create the PR in (default: GITHUB_REPOSITORY)
#                        Useful for creating PRs in a fork during testing
#   BASE_BRANCH        - Branch to create the PR against (default: main)
#   CLAUDE_MODEL       - Model to use (default: claude-sonnet-4-20250514)
#   MAX_ARTIFACTS      - Maximum number of artifacts to download (default: 10)
#   DRY_RUN            - If set to "true", skip Claude invocation and PR creation
#   SKIP_PR            - If set to "true", run Claude to make local changes but skip PR creation
#   KEEP_WORK_DIR      - If set to "true", keep the work directory in /tmp for inspection
#

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
WORK_DIR="${WORK_DIR:-/tmp/claude-fix-$$}"
MAX_ARTIFACTS="${MAX_ARTIFACTS:-10}"
CLAUDE_MODEL="${CLAUDE_MODEL:-claude-sonnet-4-20250514}"
BASE_BRANCH="${BASE_BRANCH:-main}"
DRY_RUN="${DRY_RUN:-false}"
SKIP_PR="${SKIP_PR:-false}"
KEEP_WORK_DIR="${KEEP_WORK_DIR:-false}"

# GITHUB_REPOSITORY defaults to timescale/timescaledb
GITHUB_REPOSITORY="${GITHUB_REPOSITORY:-timescale/timescaledb}"
export GITHUB_REPOSITORY

# GITHUB_TOKEN defaults to gh auth token if not set
if [[ -z "${GITHUB_TOKEN:-}" ]]; then
    GITHUB_TOKEN=$(gh auth token 2>/dev/null || true)
fi
export GITHUB_TOKEN

# TARGET_REPOSITORY defaults to GITHUB_REPOSITORY (set after env var check)

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

check_prerequisites() {
    log_info "Checking prerequisites..."

    local missing_vars=()
    [[ -z "${GITHUB_TOKEN:-}" ]] && missing_vars+=("GITHUB_TOKEN (set it or run 'gh auth login')")
    [[ -z "${GITHUB_RUN_ID:-}" ]] && missing_vars+=("GITHUB_RUN_ID")

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

    # Set TARGET_REPOSITORY (defaults to GITHUB_REPOSITORY)
    TARGET_REPOSITORY="${TARGET_REPOSITORY:-${GITHUB_REPOSITORY}}"
    export TARGET_REPOSITORY
    if [[ "${TARGET_REPOSITORY}" != "${GITHUB_REPOSITORY}" ]]; then
        log_info "Artifacts from: ${GITHUB_REPOSITORY}"
        log_info "PR will be created in: ${TARGET_REPOSITORY}"
    fi

    log_info "All prerequisites satisfied"
}

get_failed_jobs() {
    log_info "Fetching failed jobs from run ${GITHUB_RUN_ID}..."

    local jobs_file="${WORK_DIR}/failed_jobs.json"

    # Get all jobs from the run, filter for failed ones (excluding ignored failures)
    gh api "repos/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}/jobs" \
        --paginate \
        --jq '.jobs[] | select(.conclusion == "failure") | {id: .id, name: .name}' \
        > "${jobs_file}" 2>/dev/null || {
        log_error "Failed to fetch jobs list"
        return 1
    }

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

        gh api "repos/${GITHUB_REPOSITORY}/actions/jobs/${job_id}/logs" \
            > "${WORK_DIR}/logs/${safe_name}.log" 2>/dev/null || {
            log_warn "Failed to download log for job: ${job_name}"
            continue
        }
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
    gh api "repos/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}/artifacts" \
        --paginate \
        --jq ".artifacts[] | select(.name | test(\"Regression|PostgreSQL log|Stacktrace|TAP\")) | {name: .name, id: .id}" \
        > "${artifacts_file}.all" 2>/dev/null || {
        log_error "Failed to fetch artifacts list"
        return 1
    }

    # Filter artifacts matching our failed jobs
    : > "${artifacts_file}"
    while IFS= read -r artifact; do
        [[ -z "${artifact}" ]] && continue
        local name
        name=$(echo "$artifact" | jq -r '.name')
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

        local name id
        name=$(echo "$artifact" | jq -r '.name')
        id=$(echo "$artifact" | jq -r '.id')

        ((count++))
        log_info "Downloading artifact: ${name} (${count}/${total_artifacts})"
        gh api "repos/${GITHUB_REPOSITORY}/actions/artifacts/${id}/zip" > "${name}.zip" || {
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

    # Check if there are any changes to commit
    local modified_files new_files_list
    modified_files=$(git diff --name-only 2>/dev/null)
    new_files_list=$(git status --porcelain | grep '^??' | cut -c4- | while read -r file; do
        if ! grep -qxF "${file}" "${WORK_DIR}/untracked_before.txt" 2>/dev/null; then
            echo "${file}"
        fi
    done)

    if [[ -z "${modified_files}" && -z "${new_files_list}" ]]; then
        log_info "No changes made for test: ${test_name}"
        return 1
    fi

    # Filter out workflow files - these require special 'workflow' scope that we don't have
    # This is a safety measure in case Claude modifies workflow files despite instructions
    local filtered_modified filtered_new
    filtered_modified=$(echo "${modified_files}" | grep -v '^\.github/workflows/' || true)
    filtered_new=$(echo "${new_files_list}" | grep -v '^\.github/workflows/' || true)

    # Check if any workflow files were modified and warn
    local workflow_changes
    workflow_changes=$(echo "${modified_files}" | grep '^\.github/workflows/' || true)
    if [[ -n "${workflow_changes}" ]]; then
        log_warn "Skipping workflow file changes (requires 'workflow' scope):"
        echo "${workflow_changes}" | while read -r f; do
            log_warn "  - ${f}"
            git checkout -- "${f}" 2>/dev/null || true  # Revert the changes
        done >&2
    fi

    if [[ -z "${filtered_modified}" && -z "${filtered_new}" ]]; then
        log_info "No non-workflow changes made for test: ${test_name}"
        return 1
    fi

    # Stage modified files (excluding workflows)
    if [[ -n "${filtered_modified}" ]]; then
        echo "${filtered_modified}" | xargs git add >&2
    fi

    # Stage new files created by Claude (excluding workflows)
    if [[ -n "${filtered_new}" ]]; then
        echo "${filtered_new}" | xargs git add >&2
    fi

    # Extract a summary from Claude's output for the commit message
    local summary
    summary=$(tail -50 "${analysis_output}" | head -30 | tr '\n' ' ' | cut -c1-200)

    # Create commit with descriptive message (redirect to stderr to avoid capturing in return value)
    git commit -m "$(cat <<EOF
Fix test: ${test_name}

${summary}...

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>
EOF
)" >&2

    log_info "Committed fix for: ${test_name}"
    return 0
}

fix_single_test() {
    local test_name="$1"
    local test_context="$2"
    local test_number="$3"
    local total_tests="$4"

    log_info "----------------------------------------------"
    log_info "FIXING TEST ${test_number}/${total_tests}: ${test_name}"
    log_info "----------------------------------------------"

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
- NEVER modify files in .github/workflows/ - these require special permissions we don't have
- Explain your reasoning briefly

After making changes, provide a one-line summary of what was fixed.
EOF

    local prompt_size
    prompt_size=$(wc -c < "${prompt_file}")
    log_info "Prompt size: ${prompt_size} bytes"

    local start_time
    start_time=$(date +%s)

    log_info "Starting Claude Code analysis for: ${test_name}"
    if ! claude -p "$(cat "${prompt_file}")" \
        --allowedTools "Edit,Write,Read,Glob,Grep,Bash" \
        2>&1 | tee "${analysis_output}" >&2; then
        log_warn "Claude Code failed to fix test: ${test_name}"
        return 1
    fi

    local end_time duration
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    log_info "Analysis completed in ${duration} seconds"

    # Commit the changes for this test
    if commit_claude_changes "${test_name}" "${analysis_output}"; then
        return 0
    else
        return 1
    fi
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
    local branch_name
    branch_name="claude-fix/nightly-$(date +%Y%m%d-%H%M%S)"

    log_info "=============================================="
    log_info "INVOKING CLAUDE CODE"
    log_info "=============================================="

    cd "${REPO_ROOT}"

    # Save list of untracked files before Claude runs
    git status --porcelain | grep '^??' | cut -c4- > "${WORK_DIR}/untracked_before.txt"
    local untracked_count
    untracked_count=$(wc -l < "${WORK_DIR}/untracked_before.txt")
    log_info "Pre-existing untracked files: ${untracked_count}"

    # Create a new branch for the fix
    log_info "Creating branch: ${branch_name}"
    git checkout -b "${branch_name}" >&2

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
        sed 's/^[^:]*: //' "${failed_tests_file}" | tee "${WORK_DIR}/tests_without_prefix.txt" | \
            sed 's/^[0-9T:.Z-]* //' | \
            grep -oE 'test [^ ]+|^[^ ]+ \.\.\.|not ok [0-9]+\s+[+-]\s+[a-zA-Z0-9_]+|not ok [0-9]+ - [^ ]+' | \
            sed 's/^test //; s/ \.\.\..*//; s/^not ok [0-9]* - //; s/^not ok [0-9]*\s*[+-]\s*//' | \
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

            # Fix this specific test
            if fix_single_test "${test_name}" "${test_context}" "${test_number}" "${total_tests}"; then
                ((fixed_tests++))
            else
                ((failed_fixes++))
            fi

            # Update untracked files list after each commit
            git status --porcelain | grep '^??' | cut -c4- > "${WORK_DIR}/untracked_before.txt"

        done < "${unique_tests_file}"
    else
        # Fallback: if we can't identify individual tests, do one big fix
        log_info "Could not identify individual tests, attempting bulk fix..."
        total_tests=1

        local prompt_file="${WORK_DIR}/prompt.txt"
        cat > "${prompt_file}" << EOF
I need you to analyze test failures from our nightly CI run and fix them.

Here is the failure context:
$(cat "${context_file}")

Please:
1. Analyze the test failures shown above
2. Identify the root cause of each failure
3. Implement fixes for the issues in the codebase
4. If the test expectations need updating (not the code), update the expected output files

Important guidelines:
- Only fix actual bugs, don't just update test expectations to make tests pass unless the new behavior is correct
- If a test is flaky due to timing issues, make the test more robust rather than ignoring it
- NEVER modify files in .github/workflows/ - these require special permissions we don't have
- Explain your reasoning for each fix

After making changes, provide a summary of what was fixed.
EOF

        local analysis_output="${WORK_DIR}/analysis_output.txt"
        if claude -p "$(cat "${prompt_file}")" \
            --allowedTools "Edit,Write,Read,Glob,Grep,Bash" \
            2>&1 | tee "${analysis_output}" >&2; then
            if commit_claude_changes "all-failures" "${analysis_output}"; then
                ((fixed_tests++))
            else
                ((failed_fixes++))
            fi
        else
            ((failed_fixes++))
        fi
    fi

    log_info "=============================================="
    log_info "SUMMARY"
    log_info "=============================================="
    log_info "Total tests attempted: ${total_tests}"
    log_info "Successfully fixed: ${fixed_tests}"
    log_info "Failed to fix: ${failed_fixes}"

    # Show all commits made
    log_info "Commits created:"
    git log --oneline "${BASE_BRANCH}..HEAD" 2>/dev/null | while read -r line; do
        log_info "  ${line}"
    done >&2

    log_info "=============================================="

    if [[ ${fixed_tests} -eq 0 ]]; then
        log_error "No tests were fixed"
        return 1
    fi

    echo "${branch_name}"
}

create_pull_request() {
    local branch_name="$1"

    log_info "Creating pull request..."

    cd "${REPO_ROOT}"

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
    if [[ "${TARGET_REPOSITORY}" != "${GITHUB_REPOSITORY}" ]]; then
        log_info "Setting up remote for target repository: ${TARGET_REPOSITORY}"
        # Use token in URL for authentication
        local target_url="https://x-access-token:${GITHUB_TOKEN}@github.com/${TARGET_REPOSITORY}.git"
        git remote add target "${target_url}" 2>/dev/null || \
            git remote set-url target "${target_url}"
        push_remote="target"
    else
        # For same repo, also ensure origin uses token authentication
        local origin_url="https://x-access-token:${GITHUB_TOKEN}@github.com/${GITHUB_REPOSITORY}.git"
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

    # Generate commit list for PR body
    local commit_list
    commit_list=$(git log --format="- **%s**%n  %b" "${BASE_BRANCH}..HEAD" 2>/dev/null | head -100)

    # Create the PR body
    local pr_body
    pr_body=$(cat <<EOF
## Summary

This PR was automatically generated by Claude Code to fix failing nightly CI tests.
Each test fix is in a separate commit with its own description.

### Original Failure

- **Run ID**: ${GITHUB_RUN_ID}
- **Run URL**: https://github.com/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}

### Commits (${commit_count} fixes)

${commit_list}

### Testing

Please review each commit and ensure the fixes are appropriate before merging.
The CI will run the full test suite to verify the fixes.

---

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)

    # Create the PR in the target repository
    log_info "Creating PR in repository: ${TARGET_REPOSITORY}"
    log_info "Base: ${BASE_BRANCH}, Head: ${branch_name}"

    # Save PR body to file for debugging
    echo "${pr_body}" > "${WORK_DIR}/pr_body.md"
    log_info "PR body saved to: ${WORK_DIR}/pr_body.md"

    local pr_url
    local gh_output
    gh_output="${WORK_DIR}/gh_pr_output.txt"

    # Determine head ref format
    # - Same repo: just branch name
    # - Cross-repo (fork): owner:branch format
    local head_ref="${branch_name}"
    log_info "Creating PR: base=${BASE_BRANCH}, head=${head_ref}"
    log_info "Target repo: ${TARGET_REPOSITORY}"

    if gh pr create \
        --repo "${TARGET_REPOSITORY}" \
        --title "Fix nightly test failures ($(date +%Y-%m-%d))" \
        --body "${pr_body}" \
        --base "${BASE_BRANCH}" \
        --head "${head_ref}" \
        2>&1 | tee "${gh_output}"; then
        pr_url=$(grep -oE 'https://github.com/[^ ]+' "${gh_output}" | head -1)
        log_info "Pull request created: ${pr_url}"
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
    log_info "Source repository: ${GITHUB_REPOSITORY:-not set}"
    log_info "Target repository: ${TARGET_REPOSITORY:-${GITHUB_REPOSITORY:-not set}}"
    log_info "Base branch: ${BASE_BRANCH}"
    log_info "Run ID: ${GITHUB_RUN_ID:-not set}"
    log_info "Mode: $(if [[ "${DRY_RUN}" == "true" ]]; then echo "DRY_RUN"; elif [[ "${SKIP_PR}" == "true" ]]; then echo "SKIP_PR"; else echo "FULL"; fi)"

    check_prerequisites

    mkdir -p "${WORK_DIR}"

    # Step 1: Get list of failed jobs
    local jobs_file
    jobs_file=$(get_failed_jobs)
    if [[ -z "${jobs_file}" || ! -s "${jobs_file}" ]]; then
        log_error "No failed jobs found in run ${GITHUB_RUN_ID}"
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

    # Step 5: Invoke Claude Code (one commit per test)
    local branch_name
    branch_name=$(invoke_claude_code "${context_file}" "${failed_tests_file}")

    if [[ "${SKIP_PR}" == "true" ]]; then
        log_info "SKIP_PR mode - skipping PR creation"
        log_info "Changes are on branch: ${branch_name}"
        log_info "To review changes: git diff ${BASE_BRANCH}...${branch_name}"
        log_info "To push manually: git push origin ${branch_name}"
        exit 0
    fi

    # Step 6: Create PR
    local pr_url
    pr_url=$(create_pull_request "${branch_name}")

    log_info "Done! PR created: ${pr_url}"
}

main "$@"
