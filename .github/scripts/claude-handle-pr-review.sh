#!/usr/bin/env bash
#
# claude-handle-pr-review.sh
#
# This script is invoked when a review is submitted on a Claude-generated PR.
# It fetches the review feedback and uses Claude Code to address it.
#
# Usage:
#   # In GitHub Actions (environment variables set by workflow):
#   ./claude-handle-pr-review.sh
#
#   # Local testing with a specific PR:
#   ./claude-handle-pr-review.sh <pr_number>
#
#   # Local testing with options:
#   DRY_RUN=true ./claude-handle-pr-review.sh 123   # Preview without invoking Claude
#   SKIP_PUSH=true ./claude-handle-pr-review.sh 123 # Make changes but don't push
#
#   # Testing against a different repository:
#   GITHUB_REPOSITORY=timescale/timescaledb ./claude-handle-pr-review.sh 9134
#
# Required environment variables (or command-line args):
#   PR_NUMBER         - The pull request number (or first arg)
#
# Optional environment variables:
#   GITHUB_TOKEN      - GitHub token (default: from gh auth token)
#   GITHUB_REPOSITORY - The repository in owner/repo format, e.g., "timescale/timescaledb"
#                       (default: detected from git remote; git URLs are auto-normalized)
#   PR_BRANCH         - The PR branch name (default: fetched from PR)
#   REVIEWER          - Override reviewer name for commit message (default: from latest review)
#   PR_REPOSITORY     - Repository where PR lives, for API calls (default: GITHUB_REPOSITORY)
#   PUSH_REPOSITORY   - Repository to push to, for forks (default: auto-detected from PR)
#   ANTHROPIC_API_KEY - API key for Claude Code (default: use local auth)
#   CLAUDE_MODEL      - Model to use (default: claude-sonnet-4-20250514)
#   DRY_RUN           - If "true", show context but don't invoke Claude
#   SKIP_PUSH         - If "true", make changes locally but don't push
#   KEEP_WORK_DIR     - If "true", preserve work directory after completion
#

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
WORK_DIR="/tmp/claude-review-$$"
CLAUDE_MODEL="${CLAUDE_MODEL:-claude-sonnet-4-20250514}"
DRY_RUN="${DRY_RUN:-false}"
SKIP_PUSH="${SKIP_PUSH:-false}"
KEEP_WORK_DIR="${KEEP_WORK_DIR:-false}"

# Handle command-line arguments for local testing
if [[ $# -ge 1 ]]; then
    PR_NUMBER="$1"
fi

# Default GITHUB_TOKEN from gh auth if not set
if [[ -z "${GITHUB_TOKEN:-}" ]]; then
    GITHUB_TOKEN=$(gh auth token 2>/dev/null || true)
fi
export GITHUB_TOKEN

# Default GITHUB_REPOSITORY from git remote if not set
if [[ -z "${GITHUB_REPOSITORY:-}" ]]; then
    # Try to find a remote - prefer 'upstream', then 'origin', then first available
    default_remote=$(git remote 2>/dev/null | grep -E '^upstream$' || git remote 2>/dev/null | grep -E '^origin$' || git remote 2>/dev/null | head -1 || true)
    if [[ -n "${default_remote}" ]]; then
        GITHUB_REPOSITORY=$(git remote get-url "${default_remote}" 2>/dev/null | sed -E 's|.*github\.com[:/]||; s|\.git$||' || true)
    fi
fi
# Normalize GITHUB_REPOSITORY to owner/repo format (strip git URL parts if present)
GITHUB_REPOSITORY=$(echo "${GITHUB_REPOSITORY}" | sed -E 's|.*github\.com[:/]||; s|\.git$||')
export GITHUB_REPOSITORY

# PR_REPOSITORY is where the PR lives (for fetching PR info and posting comments)
# Defaults to GITHUB_REPOSITORY
PR_REPOSITORY="${PR_REPOSITORY:-${GITHUB_REPOSITORY}}"
export PR_REPOSITORY

# PUSH_REPOSITORY is where to push changes (may be a fork)
# For forks: PR is in upstream, but branch is in fork
# Defaults to PR_REPOSITORY (same repo)
PUSH_REPOSITORY="${PUSH_REPOSITORY:-${PR_REPOSITORY}}"
export PUSH_REPOSITORY

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $*" >&2; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $*" >&2; }
log_error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# Find the remote name that points to a given repository (owner/repo format)
# Returns empty string if no matching remote found
find_remote_for_repo() {
    local target_repo="$1"
    local remote remote_url remote_repo
    while IFS= read -r remote; do
        remote_url=$(git remote get-url "${remote}" 2>/dev/null || true)
        remote_repo=$(echo "${remote_url}" | sed -E 's|.*github\.com[:/]||; s|\.git$||')
        if [[ "${remote_repo}" == "${target_repo}" ]]; then
            echo "${remote}"
            return 0
        fi
    done < <(git remote 2>/dev/null)
    return 1
}

cleanup() {
    if [[ -d "${WORK_DIR}" ]]; then
        if [[ "${KEEP_WORK_DIR}" == "true" ]]; then
            log_info "Work directory preserved at: ${WORK_DIR}"
        else
            log_info "Cleaning up work directory: ${WORK_DIR}"
            rm -rf "${WORK_DIR}"
        fi
    fi
}
trap cleanup EXIT

fetch_pr_info() {
    # Fetch PR info if not provided via environment
    log_info "Fetching PR #${PR_NUMBER} info from ${PR_REPOSITORY}..."

    local pr_info
    if ! pr_info=$(gh api "repos/${PR_REPOSITORY}/pulls/${PR_NUMBER}" 2>&1); then
        log_error "Failed to fetch PR info: ${pr_info}"
        exit 1
    fi

    if [[ -z "${pr_info}" || "$(echo "${pr_info}" | jq -r '.message // empty')" == "Not Found" ]]; then
        log_error "PR #${PR_NUMBER} not found in ${PR_REPOSITORY}"
        exit 1
    fi

    if [[ -z "${PR_BRANCH:-}" ]]; then
        PR_BRANCH=$(echo "${pr_info}" | jq -r '.head.ref')
        log_info "PR branch: ${PR_BRANCH}"
    fi

    # Detect if PR is from a fork and set PUSH_REPOSITORY accordingly
    local head_repo
    head_repo=$(echo "${pr_info}" | jq -r '.head.repo.full_name')
    if [[ "${head_repo}" != "${PR_REPOSITORY}" && "${PUSH_REPOSITORY}" == "${PR_REPOSITORY}" ]]; then
        PUSH_REPOSITORY="${head_repo}"
        log_info "PR is from fork, will push to: ${PUSH_REPOSITORY}"
    fi
}

check_prerequisites() {
    log_info "Checking prerequisites..."

    local missing_vars=()
    [[ -z "${GITHUB_TOKEN:-}" ]] && missing_vars+=("GITHUB_TOKEN (set it or run 'gh auth login')")
    [[ -z "${PR_NUMBER:-}" ]] && missing_vars+=("PR_NUMBER (or pass as first argument)")
    [[ -z "${GITHUB_REPOSITORY:-}" ]] && missing_vars+=("GITHUB_REPOSITORY")

    if [[ ${#missing_vars[@]} -gt 0 ]]; then
        log_error "Missing required: ${missing_vars[*]}"
        exit 1
    fi

    for cmd in gh jq; do
        if ! command -v "$cmd" &>/dev/null; then
            log_error "Required command not found: $cmd"
            exit 1
        fi
    done

    # Claude is only required if not in DRY_RUN mode
    if [[ "${DRY_RUN}" != "true" ]]; then
        if ! command -v claude &>/dev/null; then
            log_error "Required command not found: claude"
            exit 1
        fi
    fi

    # Fetch missing PR info from API
    fetch_pr_info

    log_info "All prerequisites satisfied"
}

fetch_all_reviews() {
    log_info "Fetching all reviews for PR #${PR_NUMBER}..."

    local reviews_file="${WORK_DIR}/all_reviews.json"

    # Get all reviews (not just latest), excluding PENDING
    if ! gh api "repos/${PR_REPOSITORY}/pulls/${PR_NUMBER}/reviews" \
        --jq '[.[] | select(.state != "PENDING") | {id: .id, user: .user.login, state: .state, body: .body, submitted_at: .submitted_at}] | sort_by(.submitted_at)' \
        > "${reviews_file}" 2>&1; then
        log_warn "Failed to fetch reviews"
        echo "[]" > "${reviews_file}"
    fi

    # Get the last commit timestamp to determine which reviews are unaddressed
    local last_commit_timestamp
    last_commit_timestamp=$(git log -1 --format='%aI' 2>/dev/null || echo "1970-01-01T00:00:00Z")
    log_info "Last commit: ${last_commit_timestamp}"

    # Mark reviews as addressed or unaddressed based on commit timestamp
    local marked_reviews_file="${WORK_DIR}/marked_reviews.json"
    jq --arg last_commit "${last_commit_timestamp}" '
        map(. + {
            addressed: (.submitted_at < $last_commit)
        })
    ' "${reviews_file}" > "${marked_reviews_file}"
    mv "${marked_reviews_file}" "${reviews_file}"

    local num_reviews num_unaddressed
    num_reviews=$(jq 'length' "${reviews_file}" 2>/dev/null || echo "0")
    num_unaddressed=$(jq '[.[] | select(.addressed == false)] | length' "${reviews_file}" 2>/dev/null || echo "0")
    log_info "Found ${num_reviews} reviews (${num_unaddressed} unaddressed)"

    echo "${reviews_file}"
}

fetch_review_comments() {
    local reviews_file="$1"
    log_info "Fetching inline comments for all reviews..."

    local comments_file="${WORK_DIR}/review_comments.json"
    : > "${comments_file}"

    # Build a map of review_id -> addressed status
    local review_status
    review_status=$(jq -r '.[] | "\(.id):\(.addressed)"' "${reviews_file}" 2>/dev/null || true)

    # Fetch comments for each review, marking addressed status
    while IFS=: read -r review_id addressed; do
        [[ -z "${review_id}" ]] && continue
        gh api "repos/${PR_REPOSITORY}/pulls/${PR_NUMBER}/reviews/${review_id}/comments" \
            --jq '.[] | {review_id: '"${review_id}"', path: .path, line: .line, body: .body, diff_hunk: .diff_hunk, user: .user.login, addressed: '"${addressed}"'}' \
            >> "${comments_file}" 2>/dev/null || true
    done <<< "${review_status}"

    # Count comments
    local num_comments num_unaddressed
    num_comments=$(grep -c '^{' "${comments_file}" 2>/dev/null || echo "0")
    num_unaddressed=$(grep -c '"addressed":false' "${comments_file}" 2>/dev/null || echo "0")

    log_info "Found ${num_comments} inline comments (${num_unaddressed} unaddressed)"
    echo "${comments_file}"
}

fetch_pr_conversation() {
    log_info "Fetching PR conversation comments..."

    local conversation_file="${WORK_DIR}/pr_conversation.json"

    # Get issue comments (general PR comments, not inline)
    gh api "repos/${PR_REPOSITORY}/issues/${PR_NUMBER}/comments" \
        --jq '.[] | {user: .user.login, body: .body, created_at: .created_at}' \
        > "${conversation_file}" 2>/dev/null || true

    echo "${conversation_file}"
}

fetch_pr_diff() {
    log_info "Fetching PR diff..."

    local diff_file="${WORK_DIR}/pr_diff.patch"

    gh pr diff "${PR_NUMBER}" --repo "${PR_REPOSITORY}" > "${diff_file}" 2>/dev/null || true

    echo "${diff_file}"
}

prepare_review_context() {
    local reviews_file="$1"
    local comments_file="$2"
    local conversation_file="$3"
    local diff_file="$4"

    log_info "Preparing review context for Claude..."

    local context_file="${WORK_DIR}/review_context.md"

    cat > "${context_file}" << EOF
# PR Review Feedback

## Overview
Reviewers have submitted feedback on this Claude-generated PR. Please address all feedback.

- **PR Number**: #${PR_NUMBER}
- **Branch**: ${PR_BRANCH}

EOF

    # Add all reviews, highlighting unaddressed ones
    local num_reviews num_unaddressed
    num_reviews=$(jq 'length' "${reviews_file}" 2>/dev/null || echo "0")
    num_unaddressed=$(jq '[.[] | select(.addressed == false)] | length' "${reviews_file}" 2>/dev/null || echo "0")

    if [[ "${num_reviews}" -gt 0 ]]; then
        {
            # First, show unaddressed reviews (these need action)
            if [[ "${num_unaddressed}" -gt 0 ]]; then
                echo "## Unaddressed Reviews (ACTION REQUIRED)"
                echo ""
                echo "The following reviews were submitted AFTER the last commit and need to be addressed:"
                echo ""

                jq -r '.[] | select(.addressed == false) | "### Review by \(.user) (\(.state))\n\n\(.body // "No comment")\n"' "${reviews_file}"
            fi

            # Then, show previously addressed reviews for context
            local num_addressed
            num_addressed=$(jq '[.[] | select(.addressed == true)] | length' "${reviews_file}" 2>/dev/null || echo "0")
            if [[ "${num_addressed}" -gt 0 ]]; then
                echo "## Previous Reviews (already addressed)"
                echo ""
                echo "These reviews were submitted before the last commit (for context only):"
                echo ""

                jq -r '.[] | select(.addressed == true) | "### Review by \(.user) (\(.state))\n\n\(.body // "No comment")\n"' "${reviews_file}"
            fi
        } >> "${context_file}"
    fi

    # Add inline comments if present
    if [[ -s "${comments_file}" ]] && grep -q '^{' "${comments_file}"; then
        # Helper function to output comments
        output_comments() {
            local filter="$1"
            while IFS= read -r comment; do
                [[ -z "${comment}" || ! "${comment}" =~ ^\{ ]] && continue

                local addressed
                addressed=$(echo "${comment}" | jq -r '.addressed')
                [[ "${filter}" == "unaddressed" && "${addressed}" == "true" ]] && continue
                [[ "${filter}" == "addressed" && "${addressed}" != "true" ]] && continue

                local path line body diff_hunk user
                path=$(echo "${comment}" | jq -r '.path')
                line=$(echo "${comment}" | jq -r '.line')
                body=$(echo "${comment}" | jq -r '.body')
                diff_hunk=$(echo "${comment}" | jq -r '.diff_hunk')
                user=$(echo "${comment}" | jq -r '.user // "unknown"')

                echo "### ${path}:${line} (${user})"
                echo ""
                echo "**Comment:**"
                echo "${body}"
                echo ""
                echo "**Code context:**"
                echo '```diff'
                echo "${diff_hunk}"
                echo '```'
                echo ""
            done < "${comments_file}"
        }

        # Unaddressed inline comments (need action)
        if grep -q '"addressed":false' "${comments_file}"; then
            {
                echo "## Unaddressed Inline Comments (ACTION REQUIRED)"
                echo ""
                echo "These comments need to be addressed:"
                echo ""
                output_comments "unaddressed"
            } >> "${context_file}"
        fi

        # Addressed inline comments (for context)
        if grep -q '"addressed":true' "${comments_file}"; then
            {
                echo "## Previous Inline Comments (already addressed)"
                echo ""
                echo "These comments were addressed in previous commits (for context only):"
                echo ""
                output_comments "addressed"
            } >> "${context_file}"
        fi
    fi

    # Add recent conversation if present
    if [[ -s "${conversation_file}" ]]; then
        local recent_comments
        recent_comments=$(tail -5 "${conversation_file}")
        if [[ -n "${recent_comments}" ]]; then
            {
                echo "## Recent PR Conversation"
                echo ""
                echo '```'
                echo "${recent_comments}"
                echo '```'
                echo ""
            } >> "${context_file}"
        fi
    fi

    # Add the current PR diff for context
    if [[ -s "${diff_file}" ]]; then
        {
            echo "## Current PR Changes"
            echo ""
            echo '```diff'
            head -500 "${diff_file}"
            if [[ $(wc -l < "${diff_file}") -gt 500 ]]; then
                echo ""
                echo "... (diff truncated)"
            fi
            echo '```'
        } >> "${context_file}"
    fi

    log_info "Context prepared: ${context_file}"
    echo "${context_file}"
}

invoke_claude_for_review() {
    local context_file="$1"

    log_info "Invoking Claude Code to address review feedback..."

    cd "${REPO_ROOT}"

    local prompt_file="${WORK_DIR}/prompt.txt"
    local analysis_output="${WORK_DIR}/analysis_output.txt"

    cat > "${prompt_file}" << EOF
I need you to address PR review feedback on a pull request you previously created.

$(cat "${context_file}")

## Instructions

YOU MUST CALL TOOLS TO MAKE CHANGES. This is mandatory, not optional.

To delete a file: Call the Bash tool with command "rm <filepath>"
To edit a file: Call the Edit tool with old_string and new_string
To read a file: Call the Read tool with the file path

DO NOT just output text describing what should be done.
You MUST actually invoke tools to make changes happen.

WRONG: "I will delete the file" (no tool call = file not deleted)
RIGHT: [Calls Bash tool with "rm path/to/file"] (tool call = file deleted)

WRONG: "I edited the comment" (no tool call = nothing changed)
RIGHT: [Calls Edit tool with old and new strings] (tool call = file changed)

Important constraints:
- Make minimal, targeted changes to address the feedback
- Don't make unrelated changes or refactors
- NEVER modify files in .github/workflows/
- If you cannot address a comment, explain why

After making changes, output a summary in this exact format:

COMMIT_MESSAGE:
Address PR review feedback from ${REVIEWER}

<Brief description of changes made to address the feedback>
END_COMMIT_MESSAGE
EOF

    local prompt_size
    prompt_size=$(wc -c < "${prompt_file}")
    log_info "Prompt file created: ${prompt_file} (${prompt_size} bytes)"

    if [[ ! -s "${prompt_file}" ]]; then
        log_error "Prompt file is empty or missing!"
        return 1
    fi

    local start_time
    start_time=$(date +%s)

    log_info "Running: claude -p - --allowedTools 'Edit,Write,Read,Glob,Grep,Bash'"
    log_info "--- Claude output below ---"
    if ! claude -p - \
        --allowedTools "Edit,Write,Read,Glob,Grep,Bash" \
        < "${prompt_file}" \
        2>&1 | tee "${analysis_output}" >&2; then
        log_error "Claude Code failed to process review feedback"
        return 1
    fi
    log_info "--- Claude output above ---"

    local end_time duration
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    log_info "Analysis completed in ${duration} seconds"

    echo "${analysis_output}"
}

commit_and_push_changes() {
    local analysis_output="$1"

    log_info "Checking for changes to commit..."

    cd "${REPO_ROOT}"

    # Check for changes
    local modified_files
    modified_files=$(git diff --name-only 2>/dev/null || true)

    if [[ -z "${modified_files}" ]]; then
        log_info "No changes were made"
        return 1
    fi

    # Filter out workflow files
    local filtered_modified
    filtered_modified=$(echo "${modified_files}" | grep -v '^\.github/workflows/' || true)

    local workflow_changes
    workflow_changes=$(echo "${modified_files}" | grep '^\.github/workflows/' || true)
    if [[ -n "${workflow_changes}" ]]; then
        log_warn "Reverting workflow file changes (requires special permissions):"
        echo "${workflow_changes}" | while read -r f; do
            if [[ -n "${f}" ]]; then
                git checkout -- "${f}" 2>/dev/null || true
            fi
        done
    fi

    if [[ -z "${filtered_modified}" ]]; then
        log_info "No non-workflow changes to commit"
        return 1
    fi

    # Stage changes
    echo "${filtered_modified}" | xargs git add

    # Extract commit message from Claude's output
    local commit_msg=""
    if grep -q "COMMIT_MESSAGE:" "${analysis_output}"; then
        commit_msg=$(sed -n '/COMMIT_MESSAGE:/,/END_COMMIT_MESSAGE/p' "${analysis_output}" | \
            grep -v "COMMIT_MESSAGE:\|END_COMMIT_MESSAGE" | \
            sed 's/^[[:space:]]*//')
    fi

    if [[ -z "${commit_msg}" ]]; then
        commit_msg="Address PR review feedback from ${REVIEWER}"
    fi

    # Create commit
    git commit -m "$(cat <<EOF
${commit_msg}

Addresses feedback from review by @${REVIEWER}

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>
EOF
)"

    log_info "Committed changes"

    if [[ "${SKIP_PUSH}" == "true" ]]; then
        log_info "SKIP_PUSH mode - not pushing changes"
        log_info "Changes are committed locally on branch: ${PR_BRANCH}"
        return 0
    fi

    # Find or create a remote for pushing
    local push_remote
    push_remote=$(find_remote_for_repo "${PUSH_REPOSITORY}" || true)

    if [[ -z "${push_remote}" ]]; then
        log_info "Setting up remote for push repository: ${PUSH_REPOSITORY}"
        local push_url="https://x-access-token:${GITHUB_TOKEN}@github.com/${PUSH_REPOSITORY}.git"
        git remote add push-target "${push_url}" 2>/dev/null || \
            git remote set-url push-target "${push_url}"
        push_remote="push-target"
    else
        log_info "Using existing remote '${push_remote}' for pushing"
    fi

    # Push to the PR branch
    log_info "Pushing to ${PUSH_REPOSITORY}:${PR_BRANCH}"
    if ! git push "${push_remote}" "HEAD:${PR_BRANCH}" 2>&1; then
        log_error "Failed to push changes"
        return 1
    fi

    log_info "Successfully pushed changes"
    return 0
}

post_pr_comment() {
    local success="$1"
    local analysis_output="$2"

    log_info "Posting comment to PR..."

    local comment_body
    if [[ "${success}" == "true" ]]; then
        comment_body=$(cat <<EOF
## 🤖 Claude has addressed the review feedback

I've pushed changes to address the feedback from @${REVIEWER}.

**Changes made:**
$(git log -1 --format="%b" | head -20)

Please review the new changes and let me know if there's anything else to address.

---
*This comment was automatically generated by Claude Code in response to PR review feedback.*
EOF
)
    else
        comment_body=$(cat <<EOF
## 🤖 Claude attempted to address the review feedback

I reviewed the feedback from @${REVIEWER} but was unable to make changes automatically.

This could be because:
- The feedback requires human judgment or clarification
- The requested changes are outside the scope of automated fixes
- There was an error processing the request

Please review the feedback manually or provide more specific guidance.

---
*This comment was automatically generated by Claude Code in response to PR review feedback.*
EOF
)
    fi

    if [[ "${SKIP_PUSH}" == "true" ]]; then
        log_info "SKIP_PUSH mode - not posting PR comment"
        log_info "Would have posted:"
        echo "${comment_body}"
        return
    fi

    gh pr comment "${PR_NUMBER}" \
        --repo "${PR_REPOSITORY}" \
        --body "${comment_body}" || log_warn "Failed to post PR comment"
}

main() {
    log_info "Starting Claude PR review handler..."
    log_info "PR: #${PR_NUMBER:-not set}"
    log_info "PR Repository: ${PR_REPOSITORY:-not set}"
    log_info "Push Repository: ${PUSH_REPOSITORY:-not set}"
    log_info "Mode: $(if [[ "${DRY_RUN}" == "true" ]]; then echo "DRY_RUN"; elif [[ "${SKIP_PUSH}" == "true" ]]; then echo "SKIP_PUSH"; else echo "FULL"; fi)"

    check_prerequisites

    log_info "Branch: ${PR_BRANCH}"

    mkdir -p "${WORK_DIR}"

    # For local testing, ensure we're on the right branch
    cd "${REPO_ROOT}"
    local current_branch
    current_branch=$(git branch --show-current)
    if [[ "${current_branch}" != "${PR_BRANCH}" ]]; then
        log_info "Checking out PR branch: ${PR_BRANCH}"
        # Try to check out the branch, fetch it if needed
        if ! git checkout "${PR_BRANCH}" 2>/dev/null; then
            log_info "Branch not found locally, fetching..."

            # Find or create a remote for the source repository (where the branch lives)
            local fetch_remote
            fetch_remote=$(find_remote_for_repo "${PUSH_REPOSITORY}" || true)

            if [[ -z "${fetch_remote}" ]]; then
                log_info "Setting up remote for source repository: ${PUSH_REPOSITORY}"
                local fetch_url="https://github.com/${PUSH_REPOSITORY}.git"
                git remote add pr-source "${fetch_url}" 2>/dev/null || \
                    git remote set-url pr-source "${fetch_url}"
                fetch_remote="pr-source"
            else
                log_info "Using existing remote '${fetch_remote}' for ${PUSH_REPOSITORY}"
            fi

            # Try fetching the branch directly, or use GitHub's PR refs
            if ! git fetch "${fetch_remote}" "${PR_BRANCH}:${PR_BRANCH}" 2>/dev/null; then
                log_info "Fetching via PR ref..."
                # For PRs, GitHub provides refs/pull/NUMBER/head
                local pr_remote
                pr_remote=$(find_remote_for_repo "${PR_REPOSITORY}" || true)

                if [[ -z "${pr_remote}" ]]; then
                    local pr_url="https://github.com/${PR_REPOSITORY}.git"
                    git remote add pr-upstream "${pr_url}" 2>/dev/null || \
                        git remote set-url pr-upstream "${pr_url}"
                    pr_remote="pr-upstream"
                fi
                git fetch "${pr_remote}" "pull/${PR_NUMBER}/head:${PR_BRANCH}" || {
                    log_error "Failed to fetch PR branch"
                    exit 1
                }
            fi
            git checkout "${PR_BRANCH}"
        fi
    fi

    # Fetch all the review information
    local reviews_file comments_file conversation_file diff_file
    reviews_file=$(fetch_all_reviews)

    # Set REVIEWER from the reviews file (must be done here, not in subshell)
    if [[ -z "${REVIEWER:-}" ]]; then
        local num_unaddressed
        num_unaddressed=$(jq '[.[] | select(.addressed == false)] | length' "${reviews_file}" 2>/dev/null || echo "0")
        if [[ "${num_unaddressed}" -gt 0 ]]; then
            REVIEWER=$(jq -r '[.[] | select(.addressed == false)] | last | .user // "unknown"' "${reviews_file}")
        else
            REVIEWER=$(jq -r 'last | .user // "unknown"' "${reviews_file}" 2>/dev/null || echo "unknown")
        fi
    fi
    REVIEWER="${REVIEWER:-unknown}"
    log_info "Reviewer for commit: ${REVIEWER}"

    comments_file=$(fetch_review_comments "${reviews_file}")
    conversation_file=$(fetch_pr_conversation)
    diff_file=$(fetch_pr_diff)

    # Prepare context for Claude
    local context_file
    context_file=$(prepare_review_context "${reviews_file}" "${comments_file}" "${conversation_file}" "${diff_file}")

    if [[ "${DRY_RUN}" == "true" ]]; then
        log_info "DRY_RUN mode - showing context only"
        log_info "Work directory: ${WORK_DIR}"
        echo ""
        echo "========== REVIEW CONTEXT =========="
        cat "${context_file}"
        echo "===================================="
        KEEP_WORK_DIR=true
        return 0
    fi

    # Log context for debugging
    log_info "Review context prepared at: ${context_file}"

    # Invoke Claude to address the feedback
    local analysis_output
    analysis_output=$(invoke_claude_for_review "${context_file}")

    # Commit and push changes
    if commit_and_push_changes "${analysis_output}"; then
        post_pr_comment "true" "${analysis_output}"
        log_info "Successfully addressed PR review feedback"
    else
        post_pr_comment "false" "${analysis_output}"
        log_info "No changes made in response to review feedback"
    fi

    log_info "Done!"
}

main "$@"
