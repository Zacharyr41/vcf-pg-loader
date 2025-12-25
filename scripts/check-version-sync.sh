#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

PYPROJECT="$PROJECT_ROOT/pyproject.toml"
BIOCONDA="$PROJECT_ROOT/bioconda/meta.yaml"
MAIN_NF="$PROJECT_ROOT/nf-core/modules/vcfpgloader/load/main.nf"
ENV_YML="$PROJECT_ROOT/nf-core/modules/vcfpgloader/load/environment.yml"

extract_versions() {
    V_PYPROJECT=$(grep -E '^version = "[0-9]+\.[0-9]+\.[0-9]+"' "$PYPROJECT" | head -1 | sed 's/.*"\([0-9.]*\)".*/\1/')

    V_BIOCONDA=$(grep -E '^\{% set version = "[0-9]+\.[0-9]+\.[0-9]+" %\}' "$BIOCONDA" | sed 's/.*"\([0-9.]*\)".*/\1/')

    V_MAIN_NF=$(grep -o 'ghcr.io/zacharyr41/vcf-pg-loader:[0-9.]*' "$MAIN_NF" | head -1 | sed 's/.*://')

    V_ENV_YML=$(grep -o 'bioconda::vcf-pg-loader=[0-9.]*' "$ENV_YML" | sed 's/.*=//')
}

print_versions() {
    echo ""
    echo "Version References"
    echo "=================="
    printf "%-45s %s\n" "File" "Version"
    printf "%-45s %s\n" "----" "-------"
    printf "%-45s %s\n" "pyproject.toml" "$V_PYPROJECT"
    printf "%-45s %s\n" "bioconda/meta.yaml" "$V_BIOCONDA"
    printf "%-45s %s\n" "nf-core/.../main.nf (container)" "$V_MAIN_NF"
    printf "%-45s %s\n" "nf-core/.../environment.yml (bioconda)" "$V_ENV_YML"
    echo ""
}

check_sync() {
    local all_match=true
    local reference="$V_PYPROJECT"

    if [ -z "$reference" ]; then
        echo -e "${RED}ERROR: Could not extract version from pyproject.toml${NC}"
        exit 1
    fi

    if [ "$V_BIOCONDA" != "$reference" ]; then
        all_match=false
    fi
    if [ "$V_MAIN_NF" != "$reference" ]; then
        all_match=false
    fi
    if [ "$V_ENV_YML" != "$reference" ]; then
        all_match=false
    fi

    if [ "$all_match" = true ]; then
        echo -e "${GREEN}All versions in sync: $reference${NC}"
        return 0
    else
        echo -e "${RED}VERSION MISMATCH DETECTED${NC}"
        echo ""
        echo "Expected (from pyproject.toml): $reference"
        echo ""
        echo "Mismatches:"
        [ "$V_BIOCONDA" != "$reference" ] && echo -e "  ${YELLOW}bioconda/meta.yaml:${NC} $V_BIOCONDA"
        [ "$V_MAIN_NF" != "$reference" ] && echo -e "  ${YELLOW}nf-core/.../main.nf:${NC} $V_MAIN_NF"
        [ "$V_ENV_YML" != "$reference" ] && echo -e "  ${YELLOW}nf-core/.../environment.yml:${NC} $V_ENV_YML"
        echo ""
        echo "Run 'bump-my-version bump --new-version X.Y.Z' to synchronize all files."
        return 1
    fi
}

main() {
    extract_versions
    print_versions
    check_sync
}

main "$@"
