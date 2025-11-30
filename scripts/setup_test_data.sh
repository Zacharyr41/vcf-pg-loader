#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
CACHE_DIR="${HOME}/.cache/vcf-pg-loader-tests"
NF_CORE_TEST_DATASETS_DIR="${PROJECT_ROOT}/../test-datasets"

mkdir -p "$CACHE_DIR"

usage() {
    cat <<EOF
Usage: $(basename "$0") [COMMAND]

Commands:
  download    Download nf-core test VCFs from GitHub (fast, ~50MB)
  clone       Clone full nf-core/test-datasets repo (~2GB)
  run-sarek   Run nf-core/sarek test profile to generate outputs
  status      Show status of available test data
  clean       Remove cached test data

Environment Variables:
  NF_CORE_TEST_DATASETS  Path to local test-datasets clone

Examples:
  $(basename "$0") download     # Quick setup for CI
  $(basename "$0") clone        # Full local development setup
  $(basename "$0") run-sarek    # Generate real pipeline outputs
EOF
}

download_vcfs() {
    echo "Downloading nf-core test VCFs..."

    BASE_URL="https://raw.githubusercontent.com/nf-core/test-datasets/modules/data"

    VCFS=(
        "genomics/homo_sapiens/genome/vcf/dbsnp_146.hg38.vcf.gz"
        "genomics/homo_sapiens/genome/vcf/gnomAD.r2.1.1.vcf.gz"
        "genomics/homo_sapiens/genome/vcf/mills_and_1000G.indels.vcf.gz"
        "genomics/homo_sapiens/illumina/gatk/haplotypecaller_calls/test_haplotc.vcf.gz"
        "genomics/homo_sapiens/illumina/gatk/haplotypecaller_calls/test_haplotc.ann.vcf.gz"
        "genomics/homo_sapiens/illumina/gatk/paired_mutect2_calls/test_test2_paired_mutect2_calls.vcf.gz"
        "genomics/homo_sapiens/illumina/gatk/paired_mutect2_calls/test_test2_paired_filtered_mutect2_calls.vcf.gz"
        "genomics/homo_sapiens/illumina/vcf/genmod.vcf.gz"
        "genomics/homo_sapiens/illumina/vcf/NA12878_GIAB.chr22.vcf.gz"
    )

    for vcf in "${VCFS[@]}"; do
        dest="$CACHE_DIR/$vcf"
        mkdir -p "$(dirname "$dest")"

        if [ -f "$dest" ]; then
            echo "  [skip] $vcf (already exists)"
        else
            echo "  [download] $vcf"
            curl -sL "$BASE_URL/$vcf" -o "$dest"

            tbi_url="$BASE_URL/$vcf.tbi"
            if curl -sL --head "$tbi_url" | grep -q "200 OK"; then
                curl -sL "$tbi_url" -o "$dest.tbi"
            fi
        fi
    done

    echo "Done! VCFs cached in $CACHE_DIR"
}

clone_test_datasets() {
    if [ -d "$NF_CORE_TEST_DATASETS_DIR" ]; then
        echo "test-datasets already exists at $NF_CORE_TEST_DATASETS_DIR"
        echo "Pulling latest changes..."
        cd "$NF_CORE_TEST_DATASETS_DIR"
        git pull
    else
        echo "Cloning nf-core/test-datasets (this may take a while)..."
        git clone --depth 1 https://github.com/nf-core/test-datasets.git "$NF_CORE_TEST_DATASETS_DIR"
    fi
    echo "Done! test-datasets at $NF_CORE_TEST_DATASETS_DIR"
}

run_sarek() {
    echo "Running nf-core/sarek test profile..."

    if ! command -v nextflow &> /dev/null; then
        echo "Error: Nextflow not installed"
        echo "Install with: curl -s https://get.nextflow.io | bash"
        exit 1
    fi

    if ! command -v docker &> /dev/null; then
        echo "Error: Docker not installed"
        exit 1
    fi

    OUTPUT_DIR="$CACHE_DIR/nf_core_outputs/sarek"
    mkdir -p "$OUTPUT_DIR"

    cd "$CACHE_DIR"
    nextflow run nf-core/sarek \
        -profile test,docker \
        --outdir "$OUTPUT_DIR" \
        -resume

    echo "Sarek outputs available at $OUTPUT_DIR"
}

show_status() {
    echo "Test Data Status"
    echo "================"
    echo ""
    echo "Cache directory: $CACHE_DIR"
    if [ -d "$CACHE_DIR" ]; then
        vcf_count=$(find "$CACHE_DIR" -name "*.vcf.gz" 2>/dev/null | wc -l)
        echo "  VCFs cached: $vcf_count"
        du -sh "$CACHE_DIR" 2>/dev/null | awk '{print "  Total size: " $1}'
    else
        echo "  (not created)"
    fi
    echo ""

    echo "Local test-datasets clone:"
    if [ -d "$NF_CORE_TEST_DATASETS_DIR" ]; then
        echo "  Location: $NF_CORE_TEST_DATASETS_DIR"
        du -sh "$NF_CORE_TEST_DATASETS_DIR" 2>/dev/null | awk '{print "  Size: " $1}'
    else
        echo "  (not cloned)"
    fi
    echo ""

    echo "nf-core pipeline outputs:"
    for pipeline in sarek raredisease; do
        output_dir="$CACHE_DIR/nf_core_outputs/$pipeline"
        if [ -d "$output_dir" ]; then
            vcfs=$(find "$output_dir" -name "*.vcf.gz" 2>/dev/null | wc -l)
            echo "  $pipeline: $vcfs VCFs"
        else
            echo "  $pipeline: (not generated)"
        fi
    done
}

clean_cache() {
    echo "Cleaning test data cache..."
    rm -rf "$CACHE_DIR"
    echo "Done!"
}

case "${1:-status}" in
    download)
        download_vcfs
        ;;
    clone)
        clone_test_datasets
        ;;
    run-sarek)
        run_sarek
        ;;
    status)
        show_status
        ;;
    clean)
        clean_cache
        ;;
    -h|--help|help)
        usage
        ;;
    *)
        echo "Unknown command: $1"
        usage
        exit 1
        ;;
esac
