# GIAB Benchmark Tests

This directory contains benchmark tests using real [Genome in a Bottle (GIAB)](https://www.nist.gov/programs-projects/genome-bottle) benchmark data to validate parsing accuracy and performance.

## Data Source

Tests use the **GIAB Ashkenazi Trio** benchmark data (v4.2.1, GRCh38):
- **HG002** (NA24385) - Son/proband
- **HG003** (NA24149) - Father
- **HG004** (NA24143) - Mother

Downloaded from: https://ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release/AshkenazimTrio/

## Test Expectations

### Chr21 Subset (~2MB per sample)
Expectations derived from actual GIAB v4.2.1 data:

| Metric | HG002 Actual | Expected Range |
|--------|--------------|----------------|
| Raw VCF lines | 55,210 | - |
| After decomposition | 55,812 | 50,000-65,000 |
| SNPs | 46,489 (83.3%) | 80-90% |
| Indels | 9,323 (16.7%) | 10-20% |

Verified using:
```bash
bcftools view -H HG002_chr21.vcf.gz | wc -l           # 55,210
bcftools norm -m -any HG002_chr21.vcf.gz | wc -l      # 55,812
```

### Trio Inheritance Analysis
When comparing HG002 (proband) against parents:

| Metric | Actual | Notes |
|--------|--------|-------|
| Shared (all 3) | 33,524 | Common variants |
| Proband-Father | 42,902 | Paternal inheritance |
| Proband-Mother | 44,914 | Maternal inheritance |
| Proband-only | 1,520 (2.7%) | Includes representation differences, not true de novo |

The "proband-only" variants are NOT true de novo mutations - they arise from:
1. Different variant representation between independently-called VCFs
2. Normalization differences
3. Multi-allelic decomposition variations

True de novo rate is ~1-5 per whole genome, not per chromosome.

## Running Benchmarks

```bash
# Download GIAB chr21 data (~100MB total)
./scripts/setup_test_data.sh download-giab

# Run chr21 benchmarks (fast, ~30s)
uv run pytest -m "giab and not giab_full" -v

# Run full GIAB benchmarks (requires ~1.5GB download, slow)
./scripts/setup_test_data.sh download-giab-full
uv run pytest -m "giab_full" -v
```

## Performance Targets

| Operation | Target | Typical |
|-----------|--------|---------|
| Parsing (chr21) | >10K/sec | ~80K/sec |
| Parsing (full) | >20K/sec | ~50K/sec |
| DB Loading | >5K/sec | ~10K/sec |
| Normalization overhead | <50% | ~10-20% |

## CI Integration

The `giab-benchmark` CI job runs on pushes to main:
1. Downloads GIAB chr21 subsets (cached)
2. Runs all `@pytest.mark.giab` tests (excluding `giab_full`)
3. Reports throughput and accuracy metrics
