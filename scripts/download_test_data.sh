#!/bin/bash
set -euo pipefail

CACHE_DIR="${HOME}/.cache/vcf-pg-loader-tests"
GIAB_DIR="${CACHE_DIR}/giab"

mkdir -p "${GIAB_DIR}"

echo "=========================================="
echo "VCF-PG-Loader Test Data Download Script"
echo "=========================================="
echo ""
echo "This script downloads GIAB benchmark data for running full validation tests."
echo "Data will be cached in: ${CACHE_DIR}"
echo ""
echo "Required disk space: ~500MB for chr21 subsets, ~2GB for full files"
echo ""

GIAB_BASE_URL="https://ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release"
GIAB_VERSION="NISTv4.2.1"

HG002_URL="${GIAB_BASE_URL}/AshkenazimTrio/HG002_NA24385_son/${GIAB_VERSION}/GRCh38/HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz"
HG003_URL="${GIAB_BASE_URL}/AshkenazimTrio/HG003_NA24149_father/${GIAB_VERSION}/GRCh38/HG003_GRCh38_1_22_v4.2.1_benchmark.vcf.gz"
HG004_URL="${GIAB_BASE_URL}/AshkenazimTrio/HG004_NA24143_mother/${GIAB_VERSION}/GRCh38/HG004_GRCh38_1_22_v4.2.1_benchmark.vcf.gz"

declare -A EXPECTED_SHA256=(
)

validate_checksum() {
    local file=$1
    local expected=$2
    local sample=$3

    if [[ -z "${expected}" ]] || [[ "${expected}" == "SKIP" ]]; then
        echo "[INFO] Checksum validation skipped for ${sample}"
        echo "       GIAB does not publish official checksums. File integrity"
        echo "       relies on HTTPS transport security from NCBI servers."
        return 0
    fi

    echo "[VALIDATE] Verifying SHA256 checksum for ${sample}..."

    local actual
    if command -v sha256sum &> /dev/null; then
        actual=$(sha256sum "${file}" | cut -d' ' -f1)
    elif command -v shasum &> /dev/null; then
        actual=$(shasum -a 256 "${file}" | cut -d' ' -f1)
    else
        echo "[WARN] Neither sha256sum nor shasum found, skipping checksum validation"
        return 0
    fi

    if [[ "${actual}" != "${expected}" ]]; then
        echo "[ERROR] Checksum mismatch for ${sample}"
        echo "  Expected: ${expected}"
        echo "  Actual:   ${actual}"
        echo ""
        echo "This could indicate:"
        echo "  - Corrupted download"
        echo "  - Updated file on NCBI servers"
        echo ""
        echo "Please verify the file manually or re-download."
        rm -f "${file}"
        return 1
    fi

    echo "[OK] Checksum verified for ${sample}"
    return 0
}

download_giab_chr21() {
    local sample=$1
    local url=$2
    local output="${GIAB_DIR}/${sample}_benchmark.vcf.gz"
    local chr21_output="${GIAB_DIR}/${sample}_chr21.vcf.gz"

    if [[ -f "${chr21_output}" ]]; then
        echo "[SKIP] ${sample} chr21 already exists"
        return 0
    fi

    if ! command -v bcftools &> /dev/null; then
        echo "[ERROR] bcftools is required but not installed"
        echo "  Install with: brew install bcftools (macOS) or apt install bcftools (Linux)"
        exit 1
    fi

    echo "[DOWNLOAD] ${sample} benchmark VCF..."
    if [[ ! -f "${output}" ]]; then
        curl -L --fail -o "${output}" "${url}"
        curl -L --fail -o "${output}.tbi" "${url}.tbi" 2>/dev/null || true
    fi

    echo "[SUBSET] Extracting chr21 for ${sample}..."
    bcftools view -r chr21 "${output}" -Oz -o "${chr21_output}"
    bcftools index "${chr21_output}"

    echo "[DONE] ${sample} chr21 ready: ${chr21_output}"
}

download_giab_full() {
    local sample=$1
    local url=$2
    local output="${GIAB_DIR}/${sample}_benchmark.vcf.gz"
    local expected_hash="${EXPECTED_SHA256[$sample]:-SKIP}"

    if [[ -f "${output}" ]]; then
        echo "[SKIP] ${sample} full VCF already exists"
        return 0
    fi

    echo "[DOWNLOAD] ${sample} full benchmark VCF (~500MB)..."
    curl -L --fail -o "${output}" "${url}"
    curl -L --fail -o "${output}.tbi" "${url}.tbi" 2>/dev/null || true

    if ! validate_checksum "${output}" "${expected_hash}" "${sample}"; then
        echo "[ERROR] Checksum validation failed for ${sample}"
        exit 1
    fi

    echo "[DONE] ${sample} full VCF ready: ${output}"
}

case "${1:-chr21}" in
    chr21)
        echo "Downloading chr21 subsets (~5MB each, fast tests)..."
        echo ""
        download_giab_chr21 "HG002" "${HG002_URL}"
        download_giab_chr21 "HG003" "${HG003_URL}"
        download_giab_chr21 "HG004" "${HG004_URL}"
        ;;
    full)
        echo "Downloading full GIAB benchmark files (~500MB each)..."
        echo ""
        download_giab_full "HG002" "${HG002_URL}"
        download_giab_full "HG003" "${HG003_URL}"
        download_giab_full "HG004" "${HG004_URL}"
        ;;
    proband)
        echo "Downloading HG002 (proband) only..."
        echo ""
        download_giab_chr21 "HG002" "${HG002_URL}"
        ;;
    *)
        echo "Usage: $0 [chr21|full|proband]"
        echo ""
        echo "  chr21   - Download chr21 subsets for fast testing (default)"
        echo "  full    - Download full benchmark VCFs for complete validation"
        echo "  proband - Download HG002 only (minimal)"
        exit 1
        ;;
esac

echo ""
echo "=========================================="
echo "Download complete!"
echo "=========================================="
echo ""
echo "Run tests with:"
echo "  uv run pytest -m giab -v           # chr21 tests"
echo "  uv run pytest -m giab_full -v      # full VCF tests (slow)"
echo ""
echo "Data location: ${GIAB_DIR}"
ls -lh "${GIAB_DIR}"
