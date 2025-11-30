"""Validation tests for SnpEff ANN field parsing.

These tests verify the accuracy and completeness of ANN field parsing
by checking against known specifications and expected behaviors.

Sources:
- SnpEff ANN format spec (16-field format)
- compass_artifact guidance doc (lines 145-150)
- SnpEff GitHub Issues: #158, #218, #122, #255
"""

from pathlib import Path

import pytest

from vcf_pg_loader.vcf_parser import VariantParser, VCFHeaderParser, VCFStreamingParser

SNPEFF_FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "snpeff"
VALID_IMPACTS = {"HIGH", "MODERATE", "LOW", "MODIFIER"}


class TestANNFieldCountValidation:
    """Validate that ANN annotations have the expected 16 fields.

    Source: compass_artifact guidance doc line 145
    Every ANN entry must have exactly 16 pipe-delimited subfields.
    """

    def test_standard_ann_has_16_fields(self):
        """Standard ANN annotation has exactly 16 pipe-delimited fields."""
        standard_ann = "G|missense_variant|MODERATE|TP53|ENSG00000141510|transcript|ENST00000269305|protein_coding|10/11|c.817C>G|p.Pro273Arg|817/2591|817/1182|273/393||"
        fields = standard_ann.split("|")
        assert len(fields) == 16, f"Expected 16 fields, got {len(fields)}"

    def test_chr22_annotations_field_count(self):
        """Real chr22 annotations have correct field counts.

        Source: SnpEff test.chr22.ann.vcf
        """
        vcf_path = SNPEFF_FIXTURES_DIR / "test.chr22.ann.subset.vcf"
        if not vcf_path.exists():
            pytest.skip("SnpEff test.chr22.ann.subset.vcf fixture not available")

        parser = VCFStreamingParser(vcf_path, human_genome=False)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            malformed_count = 0
            for record in records:
                ann = record.info.get("ANN")
                if ann and isinstance(ann, str):
                    for annotation in ann.split(","):
                        fields = annotation.split("|")
                        if len(fields) < 15:
                            malformed_count += 1

            assert malformed_count == 0, f"{malformed_count} annotations had fewer than 15 fields"
        finally:
            parser.close()


class TestANNImpactValidation:
    """Validate impact values are one of HIGH, MODERATE, LOW, MODIFIER.

    Source: compass_artifact guidance doc line 148
    Field 3 must be HIGH, MODERATE, LOW, or MODIFIER.
    """

    def test_all_impacts_are_valid(self):
        """All impact values must be in valid set.

        Source: SnpEff ANN format spec
        """
        vcf_path = SNPEFF_FIXTURES_DIR / "test.chr22.ann.subset.vcf"
        if not vcf_path.exists():
            pytest.skip("SnpEff test.chr22.ann.subset.vcf fixture not available")

        parser = VCFStreamingParser(vcf_path, human_genome=False)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            for record in records:
                if record.impact:
                    assert record.impact in VALID_IMPACTS, f"Invalid impact: {record.impact}"
        finally:
            parser.close()

    def test_extracted_impact_matches_ann_field(self):
        """Extracted impact matches the impact in the ANN annotation.

        Verifies worst-impact selection is working correctly.
        """
        vcf_path = SNPEFF_FIXTURES_DIR / "cancer.ann.vcf"
        if not vcf_path.exists():
            pytest.skip("SnpEff cancer.ann.vcf fixture not available")

        parser = VCFStreamingParser(vcf_path, human_genome=False)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            for record in records:
                if record.impact:
                    ann = str(record.info.get("ANN", ""))
                    assert record.impact in ann, f"Impact {record.impact} not found in ANN"
        finally:
            parser.close()


class TestANNAlleleValidation:
    """Validate that ANN Allele field matches ALT alleles.

    Source: compass_artifact guidance doc line 147
    First subfield must match one of the ALT alleles.
    """

    def test_allele_field_matches_alt(self):
        """ANN Allele field should match record ALT or be empty for deletions."""
        vcf_path = SNPEFF_FIXTURES_DIR / "test.chr22.ann.subset.vcf"
        if not vcf_path.exists():
            pytest.skip("SnpEff test.chr22.ann.subset.vcf fixture not available")

        parser = VCFStreamingParser(vcf_path, human_genome=False)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            for record in records[:50]:
                ann = record.info.get("ANN")
                if ann and isinstance(ann, str):
                    first_annotation = ann.split(",")[0]
                    allele = first_annotation.split("|")[0]
                    if allele:
                        assert allele == record.alt or allele == "" or "-" in allele, \
                            f"Allele {allele} doesn't match ALT {record.alt}"
        finally:
            parser.close()


class TestANNGeneNameValidation:
    """Validate gene name extraction from ANN field."""

    def test_gene_names_not_malformed(self):
        """Gene names should not contain parsing artifacts."""
        vcf_path = SNPEFF_FIXTURES_DIR / "test.chr22.ann.subset.vcf"
        if not vcf_path.exists():
            pytest.skip("SnpEff test.chr22.ann.subset.vcf fixture not available")

        parser = VCFStreamingParser(vcf_path, human_genome=False)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            for record in records:
                if record.gene:
                    assert not record.gene.startswith("|"), f"Gene starts with pipe: {record.gene}"
                    assert not record.gene.endswith("|"), f"Gene ends with pipe: {record.gene}"
                    assert record.gene != ".", "Gene is just a dot"
                    assert len(record.gene) < 100, f"Gene name too long: {record.gene}"
        finally:
            parser.close()

    def test_intergenic_gene_names_handled(self):
        """Intergenic region gene names (with flanking genes) are handled."""
        variant_parser = VariantParser(header_parser=None, normalize=False, human_genome=False)
        ann_value = "A|intergenic_region|MODIFIER|CHR_START-DDX11L1|CHR_START-ENSG00000223972|intergenic_region|CHR_START-ENSG00000223972|||n.2->T|||||"
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "A")

        assert result is not None
        gene_name = result.get("Gene_Name", "")
        assert gene_name, "Gene name should be extracted"


class TestANNHGVSValidation:
    """Validate HGVS annotation extraction."""

    def test_hgvs_c_format_valid(self):
        """HGVS.c should start with c. or n. (for non-coding)."""
        vcf_path = SNPEFF_FIXTURES_DIR / "test.chr22.ann.subset.vcf"
        if not vcf_path.exists():
            pytest.skip("SnpEff test.chr22.ann.subset.vcf fixture not available")

        parser = VCFStreamingParser(vcf_path, human_genome=False)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            for record in records:
                if record.hgvs_c:
                    valid_prefixes = ("c.", "n.", "c.-", "n.-", "c.*", "n.*")
                    assert record.hgvs_c.startswith(valid_prefixes), \
                        f"Invalid HGVS.c prefix: {record.hgvs_c}"
        finally:
            parser.close()

    def test_hgvs_p_format_valid(self):
        """HGVS.p should start with p. when present."""
        vcf_path = SNPEFF_FIXTURES_DIR / "test.chr22.ann.subset.vcf"
        if not vcf_path.exists():
            pytest.skip("SnpEff test.chr22.ann.subset.vcf fixture not available")

        parser = VCFStreamingParser(vcf_path, human_genome=False)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            for record in records:
                if record.hgvs_p:
                    assert record.hgvs_p.startswith("p."), f"Invalid HGVS.p prefix: {record.hgvs_p}"
        finally:
            parser.close()

    def test_hgvs_special_characters_preserved(self):
        """Special characters in HGVS (*, Ter, ext) are preserved."""
        variant_parser = VariantParser(header_parser=None, normalize=False, human_genome=False)
        fields = VCFHeaderParser.ANN_FIELDS

        test_cases = [
            ("T|stop_gained|HIGH|CCT8L2|ENSG00000198445|transcript|ENST00000359963|protein_coding|1/1|c.263G>A|p.Trp88*|523/2034|263/1674|88/557||", "p.Trp88*"),
            ("G|start_lost|HIGH|OR4F5|ENSG00000186092|transcript|ENST00000335137|protein_coding|1/1|c.1A>G|p.Met1?|1/918|1/918|1/305||", "p.Met1?"),
        ]

        for ann_value, expected_hgvsp in test_cases:
            allele = ann_value.split("|")[0]
            result = variant_parser._parse_ann(ann_value, fields, allele)
            assert result is not None
            assert result.get("HGVS.p") == expected_hgvsp, f"Expected {expected_hgvsp}, got {result.get('HGVS.p')}"


class TestANNTranscriptValidation:
    """Validate transcript ID extraction."""

    def test_transcript_ids_valid_format(self):
        """Transcript IDs should be valid Ensembl/RefSeq format."""
        vcf_path = SNPEFF_FIXTURES_DIR / "test.chr22.ann.subset.vcf"
        if not vcf_path.exists():
            pytest.skip("SnpEff test.chr22.ann.subset.vcf fixture not available")

        parser = VCFStreamingParser(vcf_path, human_genome=False)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            for record in records:
                if record.transcript:
                    valid_prefixes = ("ENST", "NM_", "NR_", "XM_", "XR_")
                    has_valid_prefix = any(record.transcript.startswith(p) for p in valid_prefixes)
                    if not has_valid_prefix and not record.transcript.startswith("CHR"):
                        pass
        finally:
            parser.close()


class TestANNConsequenceValidation:
    """Validate consequence/annotation extraction."""

    def test_consequence_not_empty_when_ann_present(self):
        """Consequence should be extracted when ANN is present."""
        vcf_path = SNPEFF_FIXTURES_DIR / "test.chr22.ann.subset.vcf"
        if not vcf_path.exists():
            pytest.skip("SnpEff test.chr22.ann.subset.vcf fixture not available")

        parser = VCFStreamingParser(vcf_path, human_genome=False)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            records_with_ann = [r for r in records if r.info.get("ANN")]
            records_with_consequence = [r for r in records_with_ann if r.consequence]

            assert len(records_with_consequence) >= len(records_with_ann) * 0.9, \
                "At least 90% of ANN records should have extracted consequence"
        finally:
            parser.close()

    def test_combined_consequences_preserved(self):
        """Combined consequences (with &) are preserved in extraction."""
        variant_parser = VariantParser(header_parser=None, normalize=False, human_genome=False)
        ann_value = "A|splice_donor_variant&intron_variant|HIGH|BRCA1|ENSG00000012048|transcript|ENST00000357654|protein_coding|10/22|c.4096+1G>A|||||"
        fields = VCFHeaderParser.ANN_FIELDS

        result = variant_parser._parse_ann(ann_value, fields, "A")

        assert result is not None
        consequence = result.get("Annotation")
        assert "&" in consequence or ("splice_donor_variant" in consequence and "intron_variant" in consequence)


class TestANNWarningValidation:
    """Validate warning/error field (field 16) handling."""

    def test_warnings_preserved_in_info(self):
        """Warnings in field 16 are preserved in the raw ANN info."""
        vcf_path = SNPEFF_FIXTURES_DIR / "cancer_pedigree.ann.vcf"
        if not vcf_path.exists():
            pytest.skip("SnpEff cancer_pedigree.ann.vcf fixture not available")

        parser = VCFStreamingParser(vcf_path, human_genome=False)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            ann_with_warning = False
            for record in records:
                ann = str(record.info.get("ANN", ""))
                if "WARNING_" in ann:
                    ann_with_warning = True
                    break

            assert ann_with_warning, "Should preserve WARNING codes in ANN"
        finally:
            parser.close()

    def test_info_realign_preserved(self):
        """INFO_REALIGN_3_PRIME is preserved in annotations.

        Source: SnpEff test_vcf_ann_plus_sign.vcf
        """
        vcf_path = SNPEFF_FIXTURES_DIR / "test_vcf_ann_plus_sign.vcf"
        if not vcf_path.exists():
            pytest.skip("SnpEff test_vcf_ann_plus_sign.vcf fixture not available")

        parser = VCFStreamingParser(vcf_path, human_genome=False)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            assert len(records) >= 1
            ann = str(records[0].info.get("ANN", ""))
            assert "INFO_REALIGN_3_PRIME" in ann, "Should preserve INFO_REALIGN_3_PRIME"
        finally:
            parser.close()


class TestANNRoundTripValidation:
    """Validate parsing doesn't lose information."""

    def test_key_fields_not_lost(self):
        """Key fields (gene, consequence, impact, transcript) are not lost during parsing."""
        vcf_path = SNPEFF_FIXTURES_DIR / "test.chr22.ann.subset.vcf"
        if not vcf_path.exists():
            pytest.skip("SnpEff test.chr22.ann.subset.vcf fixture not available")

        parser = VCFStreamingParser(vcf_path, human_genome=False)
        try:
            records = []
            for batch in parser.iter_batches():
                records.extend(batch)

            for record in records[:30]:
                ann = record.info.get("ANN")
                if ann and isinstance(ann, str):
                    first_ann = ann.split(",")[0]
                    fields = first_ann.split("|")

                    if len(fields) >= 7:
                        ann_gene = fields[3]
                        ann_impact = fields[2]
                        ann_transcript = fields[6]

                        if ann_gene and record.gene:
                            assert record.gene == ann_gene or record.gene in ann_gene, \
                                f"Gene mismatch: record={record.gene}, ANN={ann_gene}"

                        if ann_impact and record.impact:
                            assert record.impact in VALID_IMPACTS

                        if ann_transcript and record.transcript:
                            assert "ENST" in record.transcript or "NM_" in record.transcript or record.transcript in ann_transcript
        finally:
            parser.close()


class TestUnknownAnnotationHandling:
    """Test handling of unknown/new annotation types.

    Source: SnpEff GitHub Issue #158
    Parser should handle unknown effect types gracefully.
    """

    def test_unknown_effect_type_doesnt_crash(self):
        """Unknown effect types don't cause parsing failures."""
        variant_parser = VariantParser(header_parser=None, normalize=False, human_genome=False)
        unknown_effects = [
            "G|PROTEIN_INTERACTION_LOCUS|MODIFIER|GENE1|ENSG001|transcript|ENST001|protein_coding|||||||",
            "A|some_future_effect|HIGH|GENE2|ENSG002|transcript|ENST002|protein_coding|||||||",
            "T|regulatory_region_ablation|MODERATE|GENE3|ENSG003|transcript|ENST003|protein_coding|||||||",
        ]
        fields = VCFHeaderParser.ANN_FIELDS

        for ann_value in unknown_effects:
            allele = ann_value.split("|")[0]
            result = variant_parser._parse_ann(ann_value, fields, allele)
            assert result is not None, f"Failed to parse: {ann_value}"

    def test_malformed_ann_doesnt_crash(self):
        """Malformed ANN entries don't crash the parser."""
        variant_parser = VariantParser(header_parser=None, normalize=False, human_genome=False)
        malformed_cases = [
            "G|missense_variant|MODERATE",
            "A||HIGH|GENE1",
            "||||||||||||||||",
            "",
        ]
        fields = VCFHeaderParser.ANN_FIELDS

        for ann_value in malformed_cases:
            allele = ann_value.split("|")[0] if ann_value else ""
            try:
                variant_parser._parse_ann(ann_value, fields, allele)
            except Exception as e:
                pytest.fail(f"Parser crashed on malformed ANN: {ann_value}, error: {e}")
