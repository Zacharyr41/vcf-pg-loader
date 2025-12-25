#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m'

FORK_REPO="${NFCORE_FORK:-Zacharyr41/nf-core-modules}"
UPSTREAM_REPO="nf-core/modules"
MODULE_PATH="modules/nf-core/vcfpgloader"
LOCAL_MODULE="$PROJECT_ROOT/nf-core/modules/vcfpgloader"
DRY_RUN=false
TEMP_DIR=""

cleanup() {
    if [[ -n "$TEMP_DIR" && -d "$TEMP_DIR" ]]; then
        rm -rf "$TEMP_DIR"
    fi
}
trap cleanup EXIT

usage() {
    cat <<EOF
Usage: $(basename "$0") <command> [options]

Manage nf-core/modules vcfpgloader module.

Commands:
  lint                      Run nf-core lint on local module
  diff                      Show diff against upstream nf-core/modules
  create-branch VERSION     Create new branch in fork from upstream/master
  update-branch BRANCH      Push local module to existing branch in fork
  status                    Show fork status and any open PRs

Options:
  --fork REPO               Fork repository (default: $FORK_REPO)
  --dry-run                 Show what would happen without making changes
  -h, --help                Show this help message

Environment Variables:
  NFCORE_FORK               Override default fork repository
  GITHUB_TOKEN              GitHub token for API calls (optional)

Examples:
  $(basename "$0") lint
  $(basename "$0") diff
  $(basename "$0") create-branch 0.5.4
  $(basename "$0") update-branch update-vcfpgloader-0.5.4
  $(basename "$0") status
EOF
}

cmd_lint() {
    echo -e "${BLUE}=== Linting nf-core module ===${NC}"

    if ! command -v nf-core &> /dev/null; then
        echo "Installing nf-core tools..."
        pip install nf-core --quiet
    fi

    cd "$PROJECT_ROOT"

    TEMP_DIR=$(mktemp -d)
    mkdir -p "$TEMP_DIR/$MODULE_PATH"
    cp -r "$LOCAL_MODULE/"* "$TEMP_DIR/$MODULE_PATH/"

    cd "$TEMP_DIR"
    git init --quiet
    git add .
    git commit -m "temp" --quiet

    echo ""
    nf-core modules lint vcfpgloader/load --dir . || true

    echo ""
    echo -e "${GREEN}Lint complete${NC}"
}

cmd_diff() {
    echo -e "${BLUE}=== Diff against upstream nf-core/modules ===${NC}"

    TEMP_DIR=$(mktemp -d)
    cd "$TEMP_DIR"

    echo "Fetching upstream module..."
    git clone --depth 1 --filter=blob:none --sparse "https://github.com/$UPSTREAM_REPO.git" upstream --quiet 2>/dev/null || {
        echo -e "${YELLOW}Module not found in upstream (may not be merged yet)${NC}"
        return 0
    }

    cd upstream
    git sparse-checkout set "$MODULE_PATH" 2>/dev/null || {
        echo -e "${YELLOW}Module not found in upstream nf-core/modules${NC}"
        echo "This is expected if the module hasn't been merged yet."
        return 0
    }

    if [[ ! -d "$MODULE_PATH" ]]; then
        echo -e "${YELLOW}Module not found in upstream nf-core/modules${NC}"
        return 0
    fi

    echo ""
    echo "Comparing local module to upstream..."
    echo ""

    diff -ru "$MODULE_PATH" "$LOCAL_MODULE" || true

    echo ""
    echo -e "${GREEN}Diff complete${NC}"
}

cmd_create_branch() {
    local version="$1"

    if [[ -z "$version" ]]; then
        echo -e "${RED}ERROR: VERSION required${NC}"
        echo "Usage: $(basename "$0") create-branch VERSION"
        exit 1
    fi

    if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo -e "${RED}ERROR: Invalid version format '$version'${NC}"
        exit 1
    fi

    local branch="update-vcfpgloader-$version"

    echo -e "${BLUE}=== Creating branch $branch ===${NC}"

    if [[ "$DRY_RUN" = true ]]; then
        echo "[DRY RUN] Would:"
        echo "  1. Clone fork: $FORK_REPO"
        echo "  2. Add upstream: $UPSTREAM_REPO"
        echo "  3. Create branch: $branch from upstream/master"
        echo "  4. Copy module files from: $LOCAL_MODULE"
        echo "  5. Commit and push to: origin/$branch"
        return 0
    fi

    TEMP_DIR=$(mktemp -d)
    cd "$TEMP_DIR"

    echo "Cloning fork..."
    git clone "https://github.com/$FORK_REPO.git" repo --quiet
    cd repo

    git config user.name "$(git config --global user.name || echo 'vcf-pg-loader')"
    git config user.email "$(git config --global user.email || echo 'noreply@example.com')"

    echo "Adding upstream..."
    git remote add upstream "https://github.com/$UPSTREAM_REPO.git"
    git fetch upstream master --quiet

    echo "Creating branch $branch..."
    git checkout -B "$branch" upstream/master --quiet

    echo "Copying module files..."
    mkdir -p "$MODULE_PATH/load"
    cp -r "$LOCAL_MODULE/load/"* "$MODULE_PATH/load/"

    git add "$MODULE_PATH"
    git commit -m "Add/update vcfpgloader module to $version" --quiet

    echo "Pushing to origin/$branch..."
    git push -u origin "$branch" --force

    echo ""
    echo -e "${GREEN}Branch created successfully!${NC}"
    echo ""
    echo "Next steps:"
    echo "  1. Create PR: https://github.com/$UPSTREAM_REPO/compare/master...$FORK_REPO:$branch"
    echo "  2. Or use: gh pr create --repo $UPSTREAM_REPO --head ${FORK_REPO%%/*}:$branch"
}

cmd_update_branch() {
    local branch="$1"

    if [[ -z "$branch" ]]; then
        echo -e "${RED}ERROR: BRANCH required${NC}"
        echo "Usage: $(basename "$0") update-branch BRANCH"
        exit 1
    fi

    echo -e "${BLUE}=== Updating branch $branch ===${NC}"

    if [[ "$DRY_RUN" = true ]]; then
        echo "[DRY RUN] Would:"
        echo "  1. Clone fork: $FORK_REPO"
        echo "  2. Checkout branch: $branch"
        echo "  3. Copy module files from: $LOCAL_MODULE"
        echo "  4. Commit and push to: origin/$branch"
        return 0
    fi

    TEMP_DIR=$(mktemp -d)
    cd "$TEMP_DIR"

    echo "Cloning fork..."
    git clone "https://github.com/$FORK_REPO.git" repo --quiet
    cd repo

    git config user.name "$(git config --global user.name || echo 'vcf-pg-loader')"
    git config user.email "$(git config --global user.email || echo 'noreply@example.com')"

    echo "Checking out branch $branch..."
    git fetch origin "$branch" --quiet
    git checkout "$branch" --quiet

    echo "Copying module files..."
    mkdir -p "$MODULE_PATH/load"
    rm -rf "$MODULE_PATH/load/"*
    cp -r "$LOCAL_MODULE/load/"* "$MODULE_PATH/load/"

    if git diff --quiet "$MODULE_PATH"; then
        echo -e "${YELLOW}No changes to commit${NC}"
        return 0
    fi

    git add "$MODULE_PATH"
    git commit -m "Update vcfpgloader module" --quiet

    echo "Pushing to origin/$branch..."
    git push origin "$branch"

    echo ""
    echo -e "${GREEN}Branch updated successfully!${NC}"
}

cmd_status() {
    echo -e "${BLUE}=== nf-core Module Status ===${NC}"
    echo ""

    echo "Local module: $LOCAL_MODULE"
    if [[ -d "$LOCAL_MODULE/load" ]]; then
        local version
        version=$(grep -o 'ghcr.io/zacharyr41/vcf-pg-loader:[0-9.]*' "$LOCAL_MODULE/load/main.nf" | head -1 | sed 's/.*://')
        echo "  Version: $version"
        echo "  Files:"
        ls -la "$LOCAL_MODULE/load/" | tail -n +2 | awk '{print "    " $NF}'
    else
        echo "  (not found)"
    fi

    echo ""
    echo "Fork: $FORK_REPO"

    if command -v gh &> /dev/null; then
        echo ""
        echo "Open PRs to $UPSTREAM_REPO:"
        gh pr list --repo "$UPSTREAM_REPO" --author "${FORK_REPO%%/*}" --state open 2>/dev/null || echo "  (none or unable to fetch)"

        echo ""
        echo "Recent branches in fork:"
        gh api "repos/$FORK_REPO/branches" --jq '.[].name' 2>/dev/null | grep -E 'vcfpgloader|update' | head -5 || echo "  (unable to fetch)"
    else
        echo "  (install gh CLI for more details)"
    fi
}

COMMAND=""
ARGS=()

while [[ $# -gt 0 ]]; do
    case $1 in
        --fork)
            FORK_REPO="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        lint|diff|create-branch|update-branch|status)
            COMMAND="$1"
            shift
            ;;
        *)
            ARGS+=("$1")
            shift
            ;;
    esac
done

case "$COMMAND" in
    lint)
        cmd_lint
        ;;
    diff)
        cmd_diff
        ;;
    create-branch)
        cmd_create_branch "${ARGS[0]}"
        ;;
    update-branch)
        cmd_update_branch "${ARGS[0]}"
        ;;
    status)
        cmd_status
        ;;
    "")
        echo "Error: No command specified"
        usage
        exit 1
        ;;
    *)
        echo "Error: Unknown command '$COMMAND'"
        usage
        exit 1
        ;;
esac
