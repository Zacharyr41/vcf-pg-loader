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
  download       Download nf-core test VCFs from GitHub (fast, ~50MB)
  clone          Clone full nf-core/test-datasets repo (~2GB)
  run-sarek      Run nf-core/sarek test profile to generate outputs
  download-giab  Download GIAB chr21 subsets for fast benchmarking (~100MB)
  download-giab-full  Download full GIAB Ashkenazi trio (~1.5GB)
  status         Show status of available test data
  clean          Remove cached test data

Environment Variables:
  NF_CORE_TEST_DATASETS  Path to local test-datasets clone

Examples:
  $(basename "$0") download           # Quick setup for CI
  $(basename "$0") clone              # Full local development setup
  $(basename "$0") run-sarek          # Generate real pipeline outputs
  $(basename "$0") download-giab      # Download GIAB chr21 for benchmarks
  $(basename "$0") download-giab-full # Download full GIAB trio (slow)
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
        "genomics/homo_sapiens/illumina/vcf/NA12878_GIAB.chr21_22.vcf.gz"
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
    echo ""

    echo "GIAB benchmark data:"
    GIAB_DIR="$CACHE_DIR/giab"
    if [ -d "$GIAB_DIR" ]; then
        for sample in HG002 HG003 HG004; do
            full="$GIAB_DIR/${sample}_benchmark.vcf.gz"
            chr21="$GIAB_DIR/${sample}_chr21.vcf.gz"
            if [ -f "$full" ]; then
                size=$(du -h "$full" | cut -f1)
                echo "  $sample full: $size"
            else
                echo "  $sample full: (not downloaded)"
            fi
            if [ -f "$chr21" ]; then
                size=$(du -h "$chr21" | cut -f1)
                echo "  $sample chr21: $size"
            fi
        done
    else
        echo "  (not downloaded - run 'download-giab' or 'download-giab-full')"
    fi
}

clean_cache() {
    echo "Cleaning test data cache..."
    rm -rf "$CACHE_DIR"
    echo "Done!"
}

download_giab() {
    echo "Downloading GIAB chr21 subsets for benchmarking..."

    GIAB_DIR="$CACHE_DIR/giab"
    mkdir -p "$GIAB_DIR"

    GIAB_BASE="https://ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release/AshkenazimTrio"

    SAMPLES=(
        "HG002_NA24385_son:HG002"
        "HG003_NA24149_father:HG003"
        "HG004_NA24143_mother:HG004"
    )

    for sample_info in "${SAMPLES[@]}"; do
        sample_path="${sample_info%%:*}"
        sample_name="${sample_info##*:}"
        vcf_url="${GIAB_BASE}/${sample_path}/NISTv4.2.1/GRCh38/${sample_name}_GRCh38_1_22_v4.2.1_benchmark.vcf.gz"
        dest="$GIAB_DIR/${sample_name}_benchmark.vcf.gz"

        if [ -f "$dest" ]; then
            echo "  [skip] ${sample_name} full VCF (already exists)"
        else
            echo "  [download] ${sample_name} benchmark VCF..."
            curl -L "$vcf_url" -o "$dest"
            curl -L "${vcf_url}.tbi" -o "${dest}.tbi" 2>/dev/null || true
        fi

        chr21_dest="$GIAB_DIR/${sample_name}_chr21.vcf.gz"
        if [ -f "$chr21_dest" ]; then
            echo "  [skip] ${sample_name} chr21 (already exists)"
        elif command -v bcftools &> /dev/null && [ -f "$dest" ]; then
            echo "  [subset] Creating ${sample_name} chr21 subset..."
            bcftools view -r chr21 -Oz -o "$chr21_dest" "$dest"
            bcftools index "$chr21_dest"
        else
            echo "  [warn] bcftools not found, skipping chr21 subset"
        fi
    done

    echo "Done! GIAB data cached in $GIAB_DIR"
}

download_giab_full() {
    echo "Downloading full GIAB Ashkenazi trio (this will take a while)..."

    GIAB_DIR="$CACHE_DIR/giab"
    mkdir -p "$GIAB_DIR"

    GIAB_BASE="https://ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release/AshkenazimTrio"

    SAMPLES=(
        "HG002_NA24385_son:HG002"
        "HG003_NA24149_father:HG003"
        "HG004_NA24143_mother:HG004"
    )

    for sample_info in "${SAMPLES[@]}"; do
        sample_path="${sample_info%%:*}"
        sample_name="${sample_info##*:}"
        vcf_url="${GIAB_BASE}/${sample_path}/NISTv4.2.1/GRCh38/${sample_name}_GRCh38_1_22_v4.2.1_benchmark.vcf.gz"
        bed_url="${GIAB_BASE}/${sample_path}/NISTv4.2.1/GRCh38/${sample_name}_GRCh38_1_22_v4.2.1_benchmark.bed"
        dest="$GIAB_DIR/${sample_name}_benchmark.vcf.gz"
        bed_dest="$GIAB_DIR/${sample_name}_benchmark.bed"

        if [ -f "$dest" ]; then
            echo "  [skip] ${sample_name} VCF (already exists)"
        else
            echo "  [download] ${sample_name} benchmark VCF (~500MB)..."
            curl -L "$vcf_url" -o "$dest"
            curl -L "${vcf_url}.tbi" -o "${dest}.tbi" 2>/dev/null || true
        fi

        if [ -f "$bed_dest" ]; then
            echo "  [skip] ${sample_name} BED (already exists)"
        else
            echo "  [download] ${sample_name} high-confidence BED..."
            curl -L "$bed_url" -o "$bed_dest" 2>/dev/null || true
        fi
    done

    echo "Done! Full GIAB trio cached in $GIAB_DIR"
    echo "Total size: $(du -sh "$GIAB_DIR" | cut -f1)"
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
    download-giab)
        download_giab
        ;;
    download-giab-full)
        download_giab_full
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
