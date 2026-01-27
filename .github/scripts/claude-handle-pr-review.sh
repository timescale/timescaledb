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
#   CLAUDE_BOT_USERNAME - Bot username for author verification (required, e.g., github-actions[bot])
#   DRY_RUN           - If "true", show context but don't invoke Claude
#   SKIP_PUSH         - If "true", make changes locally but don't push
#   KEEP_WORK_DIR     - If "true", preserve work directory after completion
#   ANALYSIS_OUTPUT_DIR - If set, copy analysis output files to this directory before cleanup
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
ANALYSIS_OUTPUT_DIR="${ANALYSIS_OUTPUT_DIR:-}"

# Set up debug logging early - capture all stderr to a log file
# Create work dir immediately so we can start logging
mkdir -p "${WORK_DIR}"
DEBUG_LOG="${WORK_DIR}/debug.log"
: > "${DEBUG_LOG}"  # Initialize empty log file

# Redirect stderr through tee to capture to log while still displaying
exec 2> >(tee -a "${DEBUG_LOG}" >&2)

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
    # Save analysis output if requested
    if [[ -n "${ANALYSIS_OUTPUT_DIR}" && -d "${WORK_DIR}" ]]; then
        log_info "Saving analysis output to: ${ANALYSIS_OUTPUT_DIR}"
        mkdir -p "${ANALYSIS_OUTPUT_DIR}"

        # Copy the debug log (all stderr output)
        [[ -f "${DEBUG_LOG}" ]] && cp "${DEBUG_LOG}" "${ANALYSIS_OUTPUT_DIR}/"

        # Copy analysis output files
        [[ -f "${WORK_DIR}/analysis_output.txt" ]] && cp "${WORK_DIR}/analysis_output.txt" "${ANALYSIS_OUTPUT_DIR}/"

        # Copy the review context that was sent to Claude
        [[ -f "${WORK_DIR}/review_context.md" ]] && cp "${WORK_DIR}/review_context.md" "${ANALYSIS_OUTPUT_DIR}/"

        # Copy prompt file for debugging
        [[ -f "${WORK_DIR}/prompt.txt" ]] && cp "${WORK_DIR}/prompt.txt" "${ANALYSIS_OUTPUT_DIR}/"

        # Copy review data files
        for f in "${WORK_DIR}"/*.json; do
            [[ -f "$f" ]] && cp "$f" "${ANALYSIS_OUTPUT_DIR}/"
        done

        # List what was saved
        log_info "Saved files:"
        ls -la "${ANALYSIS_OUTPUT_DIR}" >&2
    fi

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

    # Get PR author if not provided
    if [[ -z "${PR_AUTHOR:-}" ]]; then
        PR_AUTHOR=$(echo "${pr_info}" | jq -r '.user.login')
    fi
    log_info "PR author: ${PR_AUTHOR}"

    # Verify PR was created by the expected bot
    if [[ -z "${CLAUDE_BOT_USERNAME:-}" ]]; then
        log_error "CLAUDE_BOT_USERNAME is required"
        log_error "For local testing, set it to the bot that authored the PR (e.g., github-actions[bot])"
        exit 1
    fi

    # Use case-insensitive comparison since GitHub usernames are case-insensitive
    if [[ "${PR_AUTHOR,,}" != "${CLAUDE_BOT_USERNAME,,}" ]]; then
        log_error "PR #${PR_NUMBER} was not created by ${CLAUDE_BOT_USERNAME} (author: ${PR_AUTHOR})"
        log_error "This script only handles PRs created by the Claude bot"
        exit 1
    fi
    log_info "Verified PR author matches expected bot: ${CLAUDE_BOT_USERNAME}"

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
    # Include comment id so Claude can reply to specific comments
    while IFS=: read -r review_id addressed; do
        [[ -z "${review_id}" ]] && continue
        gh api "repos/${PR_REPOSITORY}/pulls/${PR_NUMBER}/reviews/${review_id}/comments" \
            --jq '.[] | {id: .id, review_id: '"${review_id}"', path: .path, line: .line, body: .body, diff_hunk: .diff_hunk, user: .user.login, addressed: '"${addressed}"'}' \
            >> "${comments_file}" || true
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
        > "${conversation_file}" || true

    echo "${conversation_file}"
}

fetch_pr_diff() {
    log_info "Fetching PR diff..."

    local diff_file="${WORK_DIR}/pr_diff.patch"

    gh pr diff "${PR_NUMBER}" --repo "${PR_REPOSITORY}" > "${diff_file}" || true

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

                local comment_id path line body diff_hunk user
                comment_id=$(echo "${comment}" | jq -r '.id')
                path=$(echo "${comment}" | jq -r '.path')
                line=$(echo "${comment}" | jq -r '.line')
                body=$(echo "${comment}" | jq -r '.body')
                diff_hunk=$(echo "${comment}" | jq -r '.diff_hunk')
                user=$(echo "${comment}" | jq -r '.user // "unknown"')

                echo "### ${path}:${line} (${user}) [comment_id: ${comment_id}]"
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

IMPORTANT: First, analyze each comment to determine if it requires action from you.

### Step 1: Classify Each Comment

For each review comment, determine which category it falls into:

1. **Actionable feedback FOR YOU**: Comments requesting code changes, fixes, or improvements
   - Examples: "Please fix this bug", "Add error handling here", "This should use X instead"
   - These require you to make changes

2. **Discussion between humans**: Comments where people are talking to each other, not to you
   - Examples: "@john what do you think?", "I agree with Sarah's point", "Let's discuss this in standup"
   - DO NOT act on these - they are not addressed to you

3. **Acknowledgements or approvals**: Comments that don't request changes
   - Examples: "LGTM", "Thanks!", "Good catch", "I see", "Makes sense"
   - No action required

4. **Questions needing clarification**: Comments asking for explanation rather than changes
   - Examples: "Why did you do it this way?", "What's the purpose of this?"
   - You may respond but don't necessarily need to change code

### Step 2: Respond to Inline Comments Directly

When addressing inline code comments, **reply directly in that comment thread** to acknowledge the specific feedback. This keeps the conversation organized.

To reply to an inline comment, use the comment_id shown in brackets (e.g., [comment_id: 123456]):
\`\`\`bash
gh api repos/${PR_REPOSITORY}/pulls/${PR_NUMBER}/comments/COMMENT_ID/replies -f body="Your reply here"
\`\`\`

**IMPORTANT: Always be friendly, patient, and polite in your responses.**

Examples of good inline replies:
- "Done! Added the comment as suggested."
- "Good catch! Fixed."
- "Thanks for the suggestion! I chose this approach because [reason]. Let me know if you'd prefer a different approach."

Keep inline replies **brief and to the point**. The commit message will have more detail.

If you disagree with feedback:
- Always provide clear, respectful motivation for your decision
- Explain the trade-offs or reasoning behind your approach
- Be open to changing if the reviewer provides additional context
- Never be dismissive or defensive

**DO NOT** post a general PR-level comment for each inline comment - reply in the thread instead.

### Step 3: Decide on Code Changes

Only make code changes for comments in category 1 (actionable feedback FOR YOU).

**IMPORTANT: Stay within the scope of the original PR.**

This PR was created to fix nightly test failures. You should ONLY make changes that:
- Directly address the original test failure
- Fix issues with the proposed solution
- Improve the fix based on reviewer feedback

You should NOT make changes that:
- Add new features unrelated to the test fix
- Refactor code outside the scope of the fix
- Address pre-existing issues that weren't part of the original failure
- Implement reviewer suggestions that go beyond fixing the test

If a reviewer requests out-of-scope changes, respond politely:
- "Thanks for the suggestion! However, that change is outside the scope of this PR, which focuses on fixing the nightly test failure. I'd recommend opening a separate PR for that improvement."
- "I appreciate the feedback! That's a good idea, but it's not directly related to the test fix this PR addresses. A separate PR would be the best way to handle that change."

If ALL comments are in categories 2-4 (discussions, acknowledgements, questions), OR if you only need to respond/explain without changing code, then:
- Respond to comments as needed (see Step 2)
- Do NOT make code changes
- Output "NO_CHANGES_NEEDED" instead of a commit message
- Briefly explain why no code changes were required

**IMPORTANT**: Use NO_CHANGES_NEEDED whenever you don't make code changes, even if you responded to comments. Only use COMMIT_MESSAGE when you actually modified files.

### Step 4: Making Changes (when needed)

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
- NEVER modify files in .github/ (workflows, scripts, etc.)
- After making C code changes, run \`make format\` to format the code
- For other code (shell scripts, Python, etc.), check scripts/ for formatting tools
- NEVER modify expected output files (.out files) directly. Instead:
  1. Modify the test SQL file (.sql) if needed
  2. Run the test to generate new output
  3. If the test passes without errors, crashes, or truncated output, copy the new output over the expected file
  4. This ensures all output changes (including unexpected ones like plan changes) are captured
- If you cannot address a comment, explain why

### Step 5: PR-Level Summary (Only When Needed)

**DO NOT** post a PR-level summary comment for small fixes. The inline replies and commit message are sufficient.

**ONLY** post a PR-level summary if:
- You made significant changes to the approach (not just small tweaks)
- Multiple reviewers asked questions that need a consolidated answer
- The changes affect the overall PR in a way that needs explanation

To post a PR-level comment (only when truly needed):
\`\`\`bash
gh pr comment ${PR_NUMBER} --repo ${PR_REPOSITORY} --body "Your summary here"
\`\`\`

### Step 6: Update PR Title/Description (If Approach Changed)

If the reviewer feedback caused you to **fundamentally change the approach**, update the PR:

\`\`\`bash
# Update PR title
gh pr edit ${PR_NUMBER} --repo ${PR_REPOSITORY} --title "New title here"

# Update PR body/description
gh pr edit ${PR_NUMBER} --repo ${PR_REPOSITORY} --body "New description here"
\`\`\`

Only do this if the fix approach changed significantly - not for minor refinements.

### Step 7: Re-request Review

After making code changes to address feedback, **always re-request a review** from the reviewer(s) so they know to look at your changes:

\`\`\`bash
gh pr edit ${PR_NUMBER} --repo ${PR_REPOSITORY} --add-reviewer ${REVIEWER}
\`\`\`

This notifies the reviewer that you've addressed their feedback and the PR is ready for another look.

### Output Format

After making changes, output a summary in this exact format:

COMMIT_MESSAGE:
Address PR review feedback from ${REVIEWER}

<Brief description of changes made to address the feedback>
END_COMMIT_MESSAGE

If no changes are needed, output:

NO_CHANGES_NEEDED
<Explanation of why no code changes were required - e.g., comments were discussions between reviewers, acknowledgements, or questions not requiring code changes>
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

    # Filter out .github/ files - workflows require special scope, scripts shouldn't be modified
    local filtered_modified
    filtered_modified=$(echo "${modified_files}" | grep -v '^\.github/' || true)

    local github_changes
    github_changes=$(echo "${modified_files}" | grep '^\.github/' || true)
    if [[ -n "${github_changes}" ]]; then
        log_warn "Reverting .github/ file changes:"
        echo "${github_changes}" | while read -r f; do
            if [[ -n "${f}" ]]; then
                log_warn "  - ${f}"
                git checkout -- "${f}" 2>/dev/null || true
            fi
        done
    fi

    if [[ -z "${filtered_modified}" ]]; then
        log_info "No changes to commit (excluding .github/)"
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

Co-Authored-By: Claude <noreply@anthropic.com>
EOF
)"

    log_info "Committed changes"

    if [[ "${SKIP_PUSH}" == "true" ]]; then
        log_info "SKIP_PUSH mode - not pushing changes"
        log_info "Changes are committed locally on branch: ${PR_BRANCH}"
        return 0
    fi

    # Always set up an authenticated remote for pushing
    # Even if an existing remote matches the repo, it may not have the token
    local push_remote="push-target"
    local push_url="https://x-access-token:${GITHUB_TOKEN}@github.com/${PUSH_REPOSITORY}.git"
    log_info "Setting up authenticated remote for push repository: ${PUSH_REPOSITORY}"
    git remote add "${push_remote}" "${push_url}" 2>/dev/null || \
        git remote set-url "${push_remote}" "${push_url}"

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

    # Only post PR-level comments for errors - Claude handles inline replies for success cases
    if [[ "${success}" == "true" || "${success}" == "no_action" ]]; then
        return
    fi

    log_info "Posting error comment to PR..."

    local comment_body
    comment_body=$(cat <<EOF
## 🤖 Claude encountered an issue

I reviewed the feedback from @${REVIEWER} but was unable to make changes automatically.

This could be because:
- The feedback requires human judgment or clarification
- The requested changes are outside the scope of automated fixes
- There was an error processing the request

Please review the feedback manually or provide more specific guidance.
EOF
)

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

    # Check if Claude determined no changes were needed
    if grep -q "NO_CHANGES_NEEDED" "${analysis_output}"; then
        log_info "Claude determined no code changes are needed"
        # Claude should have already replied to inline comments - no PR-level comment needed
        log_info "Claude handled responses inline"
    # Try to commit and push changes
    elif commit_and_push_changes "${analysis_output}"; then
        # Claude should have already replied to inline comments - no PR-level comment needed
        log_info "Successfully addressed PR review feedback"
    else
        # No changes to commit - this is NOT an error
        # Claude may have:
        # 1. Only responded to comments without needing code changes
        # 2. Found the changes were already applied
        # 3. Decided after analysis that no changes were needed
        # Claude should have already replied inline, so no error comment needed
        log_info "No code changes were made (Claude may have only responded to comments)"
    fi

    log_info "Done!"
}

main "$@"
