# vcf-pg-loader
CLI Tool for efficiently migrating VCF files into relational database (Postgres)

## Citations and Acknowledgments

This project was inspired by and builds upon several foundational tools in the genomics community:

### Primary References

**Slivar** - Rapid variant filtering:
> Pedersen, B.S., Brown, J.M., Dashnow, H. et al. Effective variant filtering and expected
> candidate variant yield in studies of rare human disease. *npj Genom. Med.* 6, 60 (2021).
> https://doi.org/10.1038/s41525-021-00227-3

**GEMINI** - Original SQL-based VCF database:
> Paila, U., Chapman, B.A., Kirchner, R., & Quinlan, A.R. GEMINI: Integrative Exploration
> of Genetic Variation and Genome Annotations. *PLoS Comput Biol* 9(7): e1003153 (2013).
> https://doi.org/10.1371/journal.pcbi.1003153

**cyvcf2** - Python VCF parsing:
> Pedersen, B.S. & Quinlan, A.R. cyvcf2: fast, flexible variant analysis with Python.
> *Bioinformatics* 33(12), 1867–1869 (2017). https://doi.org/10.1093/bioinformatics/btx057

### Supporting Tools

- **vcf2db**: https://github.com/quinlan-lab/vcf2db
- **VCF Format**: Danecek et al. (2011) https://doi.org/10.1093/bioinformatics/btr330
- **bcftools/HTSlib**: Danecek et al. (2021) https://doi.org/10.1093/gigascience/giab008
- **GIAB Benchmarks**: Zook et al. (2019) https://doi.org/10.1038/s41587-019-0074-6
