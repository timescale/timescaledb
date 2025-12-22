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
#   DRY_RUN            - If set to "true", skip PR creation
#

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
WORK_DIR="${WORK_DIR:-/tmp/claude-fix-$$}"
MAX_ARTIFACTS="${MAX_ARTIFACTS:-10}"
CLAUDE_MODEL="${CLAUDE_MODEL:-claude-sonnet-4-20250514}"
DRY_RUN="${DRY_RUN:-false}"
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

download_failure_artifacts() {
    log_info "Downloading failure artifacts from run ${GITHUB_RUN_ID}..."

    mkdir -p "${WORK_DIR}/artifacts"
    cd "${WORK_DIR}/artifacts"

    # Get list of artifacts from the failed run
    local artifacts_json
    artifacts_json=$(gh api "repos/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}/artifacts" \
        --jq '.artifacts[] | select(.name | test("Regression|PostgreSQL log|Stacktrace|TAP")) | {name: .name, id: .id}')

    if [[ -z "${artifacts_json}" ]]; then
        log_warn "No failure artifacts found"
        return 1
    fi

    local count=0
    while IFS= read -r artifact; do
        if [[ ${count} -ge ${MAX_ARTIFACTS} ]]; then
            log_warn "Reached maximum artifact limit (${MAX_ARTIFACTS})"
            break
        fi

        local name id
        name=$(echo "$artifact" | jq -r '.name')
        id=$(echo "$artifact" | jq -r '.id')

        log_info "Downloading artifact: ${name}"
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

        ((count++))
    done <<< "$(echo "${artifacts_json}" | jq -c '.')"

    log_info "Downloaded ${count} artifacts"
    return 0
}

prepare_failure_context() {
    log_info "Preparing failure context for Claude..."

    local context_file="${WORK_DIR}/failure_context.md"

    cat > "${context_file}" << 'EOF'
# Test Failure Analysis

## Overview
The nightly CI tests have failed. Below is a summary of the failures.

EOF

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

    # Add installcheck logs
    echo "## Install Check Logs" >> "${context_file}"
    echo "" >> "${context_file}"

    find "${WORK_DIR}/artifacts" -name "installcheck.log" 2>/dev/null | while read -r file; do
        if [[ -s "${file}" ]]; then
            echo "### $(basename "$(dirname "${file}")")" >> "${context_file}"
            echo '```' >> "${context_file}"
            # Extract failed tests
            grep -E "(FAILED|failed|not ok)" "${file}" | head -100 >> "${context_file}" || true
            echo '```' >> "${context_file}"
            echo "" >> "${context_file}"
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

    check_prerequisites

    mkdir -p "${WORK_DIR}"

    # Download artifacts
    if ! download_failure_artifacts; then
        log_error "Failed to download failure artifacts"
        exit 1
    fi

    # Prepare context
    local context_file
    context_file=$(prepare_failure_context)

    if [[ "${DRY_RUN}" == "true" ]]; then
        log_info "Dry run mode - skipping Claude Code invocation and PR creation"
        log_info "Context file: ${context_file}"
        cat "${context_file}"
        exit 0
    fi

    # Invoke Claude Code
    local branch_name
    branch_name=$(invoke_claude_code "${context_file}")

    # Create PR
    local pr_url
    pr_url=$(create_pull_request "${branch_name}")

    log_info "Done! PR created: ${pr_url}"
}

main "$@"
