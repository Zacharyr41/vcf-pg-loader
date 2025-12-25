#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m'

TARGET_VERSION=""
SKIP_TESTS=false

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS] [VERSION]

Pre-flight validation for releases.

Arguments:
  VERSION           Target version to release (e.g., 0.5.4)

Options:
  --skip-tests      Skip running tests
  -h, --help        Show this help message

Examples:
  $(basename "$0")                    # Check current state
  $(basename "$0") 0.5.4              # Validate release to 0.5.4
  $(basename "$0") --skip-tests 0.5.4 # Skip tests
EOF
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --skip-tests)
                SKIP_TESTS=true
                shift
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            *)
                if [[ -z "$TARGET_VERSION" ]]; then
                    TARGET_VERSION="$1"
                else
                    echo "Error: Unexpected argument '$1'"
                    usage
                    exit 1
                fi
                shift
                ;;
        esac
    done
}

check_semver() {
    if [[ -n "$TARGET_VERSION" ]]; then
        if [[ ! "$TARGET_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
            echo -e "${RED}ERROR: Invalid version format '$TARGET_VERSION'${NC}"
            echo "Expected: X.Y.Z (e.g., 0.5.4)"
            exit 1
        fi
        echo -e "${GREEN}Version format valid: $TARGET_VERSION${NC}"
    fi
}

check_version_sync() {
    echo ""
    echo -e "${BLUE}=== Version Sync Check ===${NC}"
    if "$SCRIPT_DIR/check-version-sync.sh"; then
        return 0
    else
        return 1
    fi
}

check_git_state() {
    echo ""
    echo -e "${BLUE}=== Git State Check ===${NC}"

    cd "$PROJECT_ROOT"

    local branch
    branch=$(git branch --show-current)
    echo "Current branch: $branch"

    if [[ "$branch" != "main" ]]; then
        echo -e "${YELLOW}WARNING: Not on main branch (on '$branch')${NC}"
    else
        echo -e "${GREEN}On main branch${NC}"
    fi

    if [[ -n $(git status --porcelain) ]]; then
        echo -e "${RED}ERROR: Uncommitted changes detected${NC}"
        git status --short
        return 1
    else
        echo -e "${GREEN}Working directory clean${NC}"
    fi

    git fetch origin --quiet 2>/dev/null || true
    local behind
    behind=$(git rev-list --count HEAD..origin/main 2>/dev/null || echo "0")
    if [[ "$behind" -gt 0 ]]; then
        echo -e "${YELLOW}WARNING: Branch is $behind commits behind origin/main${NC}"
    fi
}

check_bump_preview() {
    if [[ -z "$TARGET_VERSION" ]]; then
        return 0
    fi

    echo ""
    echo -e "${BLUE}=== bump-my-version Preview ===${NC}"

    cd "$PROJECT_ROOT"

    if ! command -v bump-my-version &> /dev/null; then
        if command -v uv &> /dev/null; then
            echo "Installing bump-my-version..."
            uv tool install bump-my-version
        else
            echo -e "${YELLOW}WARNING: bump-my-version not installed, skipping preview${NC}"
            return 0
        fi
    fi

    echo "Files that would be modified for version $TARGET_VERSION:"
    echo ""

    bump-my-version bump --new-version "$TARGET_VERSION" --dry-run --verbose 2>&1 | grep -E '(Would|Changing|file)' || true

    echo ""
    echo -e "${GREEN}Preview complete (no changes made)${NC}"
}

run_tests() {
    if [[ "$SKIP_TESTS" = true ]]; then
        echo ""
        echo -e "${YELLOW}=== Tests Skipped ===${NC}"
        return 0
    fi

    echo ""
    echo -e "${BLUE}=== Running Tests ===${NC}"

    cd "$PROJECT_ROOT"

    if command -v uv &> /dev/null; then
        uv run pytest -v --tb=short -x -q 2>&1 | tail -20
    elif command -v pytest &> /dev/null; then
        pytest -v --tb=short -x -q 2>&1 | tail -20
    else
        echo -e "${YELLOW}WARNING: pytest not found, skipping tests${NC}"
        return 0
    fi
}

print_summary() {
    echo ""
    echo -e "${BLUE}=== Summary ===${NC}"

    if [[ -n "$TARGET_VERSION" ]]; then
        echo "Target version: $TARGET_VERSION"
        echo ""
        echo "Next steps:"
        echo "  1. Run: bump-my-version bump --new-version $TARGET_VERSION"
        echo "  2. Push: git push origin HEAD && git push origin v$TARGET_VERSION"
        echo "  3. Or trigger release workflow with version=$TARGET_VERSION"
    else
        echo "Current state validated."
        echo ""
        echo "To validate a specific release version, run:"
        echo "  $(basename "$0") X.Y.Z"
    fi
}

main() {
    parse_args "$@"

    echo -e "${BLUE}vcf-pg-loader Release Validation${NC}"
    echo "================================="

    if [[ -n "$TARGET_VERSION" ]]; then
        echo "Target version: $TARGET_VERSION"
    fi

    local errors=0

    check_semver || ((errors++))
    check_version_sync || ((errors++))
    check_git_state || ((errors++))
    check_bump_preview || ((errors++))
    run_tests || ((errors++))

    print_summary

    if [[ $errors -gt 0 ]]; then
        echo ""
        echo -e "${RED}Validation completed with $errors error(s)${NC}"
        exit 1
    else
        echo ""
        echo -e "${GREEN}Validation passed${NC}"
        exit 0
    fi
}

main "$@"
