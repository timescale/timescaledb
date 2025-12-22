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
#   GITHUB_TOKEN       - GitHub token with repo permissions
#   GITHUB_REPOSITORY  - The source repository for artifacts (e.g., timescale/timescaledb)
#   GITHUB_RUN_ID      - The workflow run ID
#
# Optional environment variables:
#   ANTHROPIC_API_KEY  - API key for Claude Code (not needed if logged in locally)
#   TARGET_REPOSITORY  - Repository to create the PR in (default: GITHUB_REPOSITORY)
#                        Useful for creating PRs in a fork during testing
#   CLAUDE_MODEL       - Model to use (default: claude-sonnet-4-20250514)
#   MAX_ARTIFACTS      - Maximum number of artifacts to download (default: 10)
#   DRY_RUN            - If set to "true", skip Claude invocation and PR creation
#   LOCAL_ONLY         - If set to "true", run Claude to make local changes but skip PR creation
#

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
WORK_DIR="${WORK_DIR:-/tmp/claude-fix-$$}"
MAX_ARTIFACTS="${MAX_ARTIFACTS:-10}"
CLAUDE_MODEL="${CLAUDE_MODEL:-claude-sonnet-4-20250514}"
DRY_RUN="${DRY_RUN:-false}"
LOCAL_ONLY="${LOCAL_ONLY:-false}"
# TARGET_REPOSITORY defaults to GITHUB_REPOSITORY (set after env var check)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $*"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*" >&2
}

cleanup() {
    if [[ -d "${WORK_DIR}" ]]; then
        log_info "Cleaning up work directory: ${WORK_DIR}"
        rm -rf "${WORK_DIR}"
    fi
}

trap cleanup EXIT

check_prerequisites() {
    log_info "Checking prerequisites..."

    local missing_vars=()
    [[ -z "${GITHUB_TOKEN:-}" ]] && missing_vars+=("GITHUB_TOKEN")
    [[ -z "${GITHUB_REPOSITORY:-}" ]] && missing_vars+=("GITHUB_REPOSITORY")
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
    > "${failed_tests_file}"

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

    # Build a pattern to match artifacts for failed jobs only
    local failed_job_patterns=""
    while IFS= read -r job; do
        [[ -z "${job}" ]] && continue
        local job_name
        job_name=$(echo "$job" | jq -r '.name')
        # Escape special regex characters in job name
        local escaped_name
        escaped_name=$(echo "${job_name}" | sed 's/[.[\*^$()+?{|]/\\&/g')
        if [[ -n "${failed_job_patterns}" ]]; then
            failed_job_patterns="${failed_job_patterns}|${escaped_name}"
        else
            failed_job_patterns="${escaped_name}"
        fi
    done < "${jobs_file}"

    if [[ -z "${failed_job_patterns}" ]]; then
        log_warn "No failed job patterns to match"
        return 1
    fi

    log_info "Fetching artifact list (with pagination)..."
    local artifacts_file="${WORK_DIR}/artifacts.json"

    # Get artifacts that match failed jobs and are relevant types
    gh api "repos/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}/artifacts" \
        --paginate \
        --jq ".artifacts[] | select(.name | test(\"Regression|PostgreSQL log|Stacktrace|TAP\")) | select(.name | test(\"${failed_job_patterns}\")) | {name: .name, id: .id}" \
        > "${artifacts_file}" 2>/dev/null || {
        log_error "Failed to fetch artifacts list"
        return 1
    }

    if [[ ! -s "${artifacts_file}" ]]; then
        log_warn "No failure artifacts found for failed jobs"
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
    echo "## Failed Jobs" >> "${context_file}"
    echo "" >> "${context_file}"

    if [[ -f "${WORK_DIR}/failed_jobs.json" ]]; then
        echo '```' >> "${context_file}"
        jq -r '.name' "${WORK_DIR}/failed_jobs.json" >> "${context_file}"
        echo '```' >> "${context_file}"
        echo "" >> "${context_file}"
    fi

    # Add failed tests from job logs (excluding ignored)
    echo "## Failed Tests (from job logs)" >> "${context_file}"
    echo "" >> "${context_file}"

    if [[ -f "${failed_tests_file}" && -s "${failed_tests_file}" ]]; then
        echo '```' >> "${context_file}"
        cat "${failed_tests_file}" >> "${context_file}"
        echo '```' >> "${context_file}"
    else
        echo "No specific failed tests identified in job logs." >> "${context_file}"
    fi
    echo "" >> "${context_file}"

    # Add regression diffs
    echo "## Regression Diffs" >> "${context_file}"
    echo "" >> "${context_file}"

    find "${WORK_DIR}/artifacts" -name "regression.log" -o -name "*.diffs" 2>/dev/null | while read -r file; do
        if [[ -s "${file}" ]]; then
            echo "### $(basename "$(dirname "${file}")")" >> "${context_file}"
            echo '```diff' >> "${context_file}"
            head -500 "${file}" >> "${context_file}"
            if [[ $(wc -l < "${file}") -gt 500 ]]; then
                echo "" >> "${context_file}"
                echo "... (truncated, ${file} has $(wc -l < "${file}") lines total)" >> "${context_file}"
            fi
            echo '```' >> "${context_file}"
            echo "" >> "${context_file}"
        fi
    done

    # Add installcheck logs (failures only, excluding ignored)
    echo "## Install Check Failures" >> "${context_file}"
    echo "" >> "${context_file}"

    find "${WORK_DIR}/artifacts" -name "installcheck.log" 2>/dev/null | while read -r file; do
        if [[ -s "${file}" ]]; then
            # Extract failed tests, excluding ignored ones
            local failures
            failures=$(grep -E "(FAILED|not ok)" "${file}" 2>/dev/null | grep -v "(ignored)" | head -100 || true)
            if [[ -n "${failures}" ]]; then
                echo "### $(basename "$(dirname "${file}")")" >> "${context_file}"
                echo '```' >> "${context_file}"
                echo "${failures}" >> "${context_file}"
                echo '```' >> "${context_file}"
                echo "" >> "${context_file}"
            fi
        fi
    done

    # Add stack traces if present
    echo "## Stack Traces" >> "${context_file}"
    echo "" >> "${context_file}"

    find "${WORK_DIR}/artifacts" -name "stacktrace.log" 2>/dev/null | while read -r file; do
        if [[ -s "${file}" ]]; then
            echo "### $(basename "$(dirname "${file}")")" >> "${context_file}"
            echo '```' >> "${context_file}"
            head -200 "${file}" >> "${context_file}"
            echo '```' >> "${context_file}"
            echo "" >> "${context_file}"
        fi
    done

    # Add PostgreSQL log excerpts (errors only)
    echo "## PostgreSQL Errors" >> "${context_file}"
    echo "" >> "${context_file}"

    find "${WORK_DIR}/artifacts" -name "postmaster.*" 2>/dev/null | while read -r file; do
        if [[ -s "${file}" ]]; then
            local errors
            errors=$(grep -E "(ERROR|FATAL|PANIC)" "${file}" 2>/dev/null | head -50 || true)
            if [[ -n "${errors}" ]]; then
                echo "### $(basename "$(dirname "${file}")")" >> "${context_file}"
                echo '```' >> "${context_file}"
                echo "${errors}" >> "${context_file}"
                echo '```' >> "${context_file}"
                echo "" >> "${context_file}"
            fi
        fi
    done

    log_info "Context prepared: ${context_file}"
    echo "${context_file}"
}

invoke_claude_code() {
    local context_file="$1"
    local branch_name="claude-fix/nightly-$(date +%Y%m%d-%H%M%S)"

    log_info "Invoking Claude Code to analyze failures..."

    cd "${REPO_ROOT}"

    # Create a new branch for the fix
    git checkout -b "${branch_name}"

    # Create a prompt file for Claude Code
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
5. Focus on the most critical failures first

Important guidelines:
- Only fix actual bugs, don't just update test expectations to make tests pass unless the new behavior is correct
- If a test is flaky due to timing issues, make the test more robust rather than ignoring it
- Explain your reasoning for each fix

After making changes, provide a summary of what was fixed.
EOF

    # Run Claude Code with the prompt
    # Using -p for non-interactive mode with a prompt
    # --allowedTools ensures Claude can edit files
    local analysis_output="${WORK_DIR}/analysis_output.txt"
    if ! claude -p "$(cat "${prompt_file}")" \
        --allowedTools "Edit,Write,Read,Glob,Grep,Bash" \
        2>&1 | tee "${analysis_output}"; then
        log_error "Claude Code failed to analyze the failures"
        return 1
    fi

    # Save the analysis for the PR
    cp "${analysis_output}" "${REPO_ROOT}/.claude-analysis.txt" 2>/dev/null || true

    echo "${branch_name}"
}

create_pull_request() {
    local branch_name="$1"

    log_info "Creating pull request..."

    cd "${REPO_ROOT}"

    # Check if there are any changes
    if git diff --quiet && git diff --staged --quiet; then
        log_warn "No changes were made by Claude Code"
        return 1
    fi

    # Read the analysis output if available
    local analysis_summary=""
    if [[ -f "${REPO_ROOT}/.claude-analysis.txt" ]]; then
        # Extract just the summary portion (last part of the output)
        analysis_summary=$(tail -100 "${REPO_ROOT}/.claude-analysis.txt" | head -80)
        # Clean up the analysis file before committing
        rm -f "${REPO_ROOT}/.claude-analysis.txt"
    fi

    # Stage and commit changes
    git add -A
    git commit -m "$(cat <<'EOF'
Fix nightly test failures

This PR was automatically generated by Claude Code to fix failing
nightly CI tests.

See the workflow run for details on the original failures.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>
EOF
)"

    # Set up the remote for the target repository if different from source
    local push_remote="origin"
    if [[ "${TARGET_REPOSITORY}" != "${GITHUB_REPOSITORY}" ]]; then
        log_info "Setting up remote for target repository: ${TARGET_REPOSITORY}"
        git remote add target "https://github.com/${TARGET_REPOSITORY}.git" 2>/dev/null || \
            git remote set-url target "https://github.com/${TARGET_REPOSITORY}.git"
        push_remote="target"
    fi

    # Push the branch
    git push "${push_remote}" "${branch_name}"

    # Create the PR body
    local pr_body
    pr_body=$(cat <<EOF
## Summary

This PR was automatically generated by Claude Code to fix failing nightly CI tests.

### Original Failure

- **Run ID**: ${GITHUB_RUN_ID}
- **Run URL**: https://github.com/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}

### Analysis and Changes

<details>
<summary>Claude Code Analysis</summary>

\`\`\`
${analysis_summary}
\`\`\`

</details>

### Testing

Please review the changes and ensure the fixes are appropriate before merging.
The CI will run the full test suite to verify the fixes.

---

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)

    # Create the PR in the target repository
    local pr_url
    pr_url=$(gh pr create \
        --repo "${TARGET_REPOSITORY}" \
        --title "Fix nightly test failures ($(date +%Y-%m-%d))" \
        --body "${pr_body}" \
        --base main \
        --head "${branch_name}")

    log_info "Pull request created: ${pr_url}"
    echo "${pr_url}"
}

main() {
    log_info "Starting Claude failure analysis..."
    log_info "Source repository: ${GITHUB_REPOSITORY:-not set}"
    log_info "Target repository: ${TARGET_REPOSITORY:-${GITHUB_REPOSITORY:-not set}}"
    log_info "Run ID: ${GITHUB_RUN_ID:-not set}"
    log_info "Mode: $(if [[ "${DRY_RUN}" == "true" ]]; then echo "DRY_RUN"; elif [[ "${LOCAL_ONLY}" == "true" ]]; then echo "LOCAL_ONLY"; else echo "FULL"; fi)"

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
        cat "${context_file}"
        exit 0
    fi

    # Step 5: Invoke Claude Code
    local branch_name
    branch_name=$(invoke_claude_code "${context_file}")

    if [[ "${LOCAL_ONLY}" == "true" ]]; then
        log_info "Local only mode - skipping PR creation"
        log_info "Changes are on branch: ${branch_name}"
        log_info "To review changes: git diff main...${branch_name}"
        log_info "To push manually: git push origin ${branch_name}"
        exit 0
    fi

    # Step 6: Create PR
    local pr_url
    pr_url=$(create_pull_request "${branch_name}")

    log_info "Done! PR created: ${pr_url}"
}

main "$@"
