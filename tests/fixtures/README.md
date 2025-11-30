# Test Fixtures

## Attribution

### slivar
The following VCF test files are sourced from **slivar** (https://github.com/brentp/slivar):

- `with_annotations.vcf` - Original: `tests/with-bcsq.vcf`
- `multiallelic.vcf` - Original: `tests/bug.vcf`

> Pedersen, B.S., Brown, J.M., Dashnow, H. et al. Effective variant filtering and expected
> candidate variant yield in studies of rare human disease. *npj Genom. Med.* 6, 60 (2021).
> https://doi.org/10.1038/s41525-021-00227-3

### nf-core/test-datasets
The following VCF test files are sourced from **nf-core/test-datasets** (https://github.com/nf-core/test-datasets):

**variantprioritization branch:**
- `mutect2_chr22.vcf.gz` - Mutect2 somatic variants (chr22)
- `strelka_snvs_chr22.vcf.gz` - Strelka2 somatic SNVs (chr22)
- `strelka_indels_chr22.vcf.gz` - Strelka2 somatic indels (chr22)

**modules branch:**
- `annotated_ranked.vcf.gz` - Clinical annotation pipeline output with VEP CSQ, CADD, genetic models
- `genmod_sv.vcf.gz` - Structural variants with VEP annotations
- `gnomad_subset.vcf.gz` - gnomAD population frequency subset
- `gvcf_sample.vcf.gz` - gVCF format with NON_REF alleles
- `mills_indels.vcf.gz` - Mills & 1000G gold standard indels
- `empty.vcf.gz` - Empty VCF (header only, no variants)
- `gridss_sv.vcf` - GRIDSS structural variants with BND format
- `sarscov2.vcf.gz` - SARS-CoV-2 variants (non-human)
- `dbsnp_subset.vcf.gz` - dbSNP VCF 4.0 format subset
- `pacbio_repeats.vcf.gz` - PacBio PBSV repeat annotations

> nf-core community. nf-core/test-datasets: Shared test data for nf-core pipelines.
> https://github.com/nf-core/test-datasets
