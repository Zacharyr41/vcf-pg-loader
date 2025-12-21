"""Tests for VCF header sanitization."""

from vcf_pg_loader.phi.header_sanitizer import (
    PHIScanner,
    SanitizationConfig,
    VCFHeaderSanitizer,
)


class TestSanitizationConfig:
    def test_default_config(self):
        config = SanitizationConfig()
        assert config.remove_commandline is True
        assert config.remove_sample_metadata is True
        assert config.remove_dates is True
        assert config.remove_file_paths is True
        assert config.remove_institution_patterns is True
        assert config.custom_patterns == []
        assert "reference" in config.preserve_fields

    def test_custom_config(self):
        config = SanitizationConfig(
            remove_commandline=False,
            custom_patterns=[r"CUSTOM_\d+"],
            preserve_fields={"myfield"},
        )
        assert config.remove_commandline is False
        assert r"CUSTOM_\d+" in config.custom_patterns
        assert "myfield" in config.preserve_fields


class TestVCFHeaderSanitizer:
    def test_sanitize_patient_id(self):
        sanitizer = VCFHeaderSanitizer()
        header = "##source=MyTool\n##PatientID=12345\n#CHROM\tPOS"
        result = sanitizer.sanitize_header(header)

        assert result.phi_detected is True
        assert len(result.removed_items) == 1
        assert result.removed_items[0].pattern_matched == "patient_id"

    def test_sanitize_mrn(self):
        sanitizer = VCFHeaderSanitizer()
        header = '##INFO=<ID=Test,Description="MRN=123456">\n#CHROM'
        result = sanitizer.sanitize_header(header)

        assert result.phi_detected is True
        assert any(item.pattern_matched == "mrn" for item in result.removed_items)

    def test_sanitize_ssn_pattern(self):
        sanitizer = VCFHeaderSanitizer()
        header = "##source=Tool containing 123-45-6789\n#CHROM"
        result = sanitizer.sanitize_header(header)

        assert result.phi_detected is True
        assert any(item.pattern_matched == "ssn_format" for item in result.removed_items)

    def test_sanitize_commandline(self):
        sanitizer = VCFHeaderSanitizer()
        header = '##CommandLine="/home/jdoe/patient_data/run.sh --sample JohnDoe"\n#CHROM'
        result = sanitizer.sanitize_header(header)

        assert result.phi_detected is True
        assert any(item.pattern_matched == "commandline" for item in result.removed_items)
        assert "[REDACTED]" in result.sanitized_lines[0]

    def test_sanitize_unix_path(self):
        sanitizer = VCFHeaderSanitizer()
        header = "##source=processed at /home/jdoe/projects/vcf\n#CHROM"
        result = sanitizer.sanitize_header(header)

        assert result.phi_detected is True
        assert any(item.pattern_matched == "unix_home_path" for item in result.removed_items)

    def test_sanitize_macos_path(self):
        sanitizer = VCFHeaderSanitizer()
        header = "##source=processed at /Users/johndoe/data\n#CHROM"
        result = sanitizer.sanitize_header(header)

        assert result.phi_detected is True
        assert any(item.pattern_matched == "macos_home_path" for item in result.removed_items)

    def test_sanitize_date_pattern(self):
        sanitizer = VCFHeaderSanitizer()
        header = "##fileDate=2024-01-15\n#CHROM"
        result = sanitizer.sanitize_header(header)

        assert result.phi_detected is True
        assert any(item.pattern_matched == "iso_date" for item in result.removed_items)

    def test_sanitize_institution(self):
        sanitizer = VCFHeaderSanitizer()
        header = "##source=Mayo Clinic Genomics Lab\n#CHROM"
        result = sanitizer.sanitize_header(header)

        assert result.phi_detected is True
        assert any(item.pattern_matched == "mayo_clinic" for item in result.removed_items)

    def test_preserve_reference_field(self):
        sanitizer = VCFHeaderSanitizer()
        header = "##reference=/data/reference/hg38.fa\n#CHROM"
        result = sanitizer.sanitize_header(header)

        assert "reference" in result.sanitized_lines[0]

    def test_no_phi_detected(self):
        sanitizer = VCFHeaderSanitizer()
        header = "##fileformat=VCFv4.2\n##INFO=<ID=DP,Number=1,Type=Integer>\n#CHROM"
        result = sanitizer.sanitize_header(header)

        assert result.phi_detected is False
        assert len(result.removed_items) == 0

    def test_custom_pattern(self):
        config = SanitizationConfig(custom_patterns=[r"INTERNAL_ID_\d+"])
        sanitizer = VCFHeaderSanitizer(config)
        header = "##sample=INTERNAL_ID_12345\n#CHROM"
        result = sanitizer.sanitize_header(header)

        assert result.phi_detected is True
        assert any(item.pattern_matched == "custom" for item in result.removed_items)

    def test_sanitize_sample_metadata(self):
        sanitizer = VCFHeaderSanitizer()
        metadata = {
            "PatientID": "12345",
            "MRN": "A123456",
            "Gender": "Male",
            "Diagnosis": "Test diagnosis from /home/jdoe/reports",
        }
        sanitized, removed = sanitizer.sanitize_sample_metadata(metadata)

        assert "PatientID" not in sanitized
        assert "MRN" not in sanitized
        assert "Gender" in sanitized
        assert "PatientID" in removed
        assert "MRN" in removed
        assert "[REDACTED]" in sanitized["Diagnosis"]

    def test_summary_counts(self):
        sanitizer = VCFHeaderSanitizer()
        header = (
            "##source=/home/user1/data\n"
            "##other=/home/user2/data\n"
            "##date=2024-01-15\n"
            "#CHROM"
        )
        result = sanitizer.sanitize_header(header)

        assert result.summary.get("unix_home_path", 0) == 2
        assert result.summary.get("iso_date", 0) == 1


class TestPHIScanner:
    def test_scan_clean_vcf(self, tmp_path):
        vcf_content = (
            "##fileformat=VCFv4.2\n"
            '##INFO=<ID=DP,Number=1,Type=Integer,Description="Depth">\n'
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
            "chr1\t100\t.\tA\tG\t30\tPASS\tDP=10\n"
        )
        vcf_file = tmp_path / "clean.vcf"
        vcf_file.write_text(vcf_content)

        scanner = PHIScanner()
        result = scanner.scan_vcf_for_phi(vcf_file)

        assert result.has_phi is False
        assert result.risk_level == "none"

    def test_scan_phi_vcf(self, tmp_path):
        vcf_content = (
            "##fileformat=VCFv4.2\n"
            "##PatientID=12345\n"
            "##source=/home/jdoe/patient_data/process.sh\n"
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
            "chr1\t100\t.\tA\tG\t30\tPASS\tDP=10\n"
        )
        vcf_file = tmp_path / "phi.vcf"
        vcf_file.write_text(vcf_content)

        scanner = PHIScanner()
        result = scanner.scan_vcf_for_phi(vcf_file)

        assert result.has_phi is True
        assert len(result.findings) >= 2

    def test_scan_high_risk(self, tmp_path):
        vcf_content = (
            "##fileformat=VCFv4.2\n"
            "##SSN=123-45-6789\n"
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
        )
        vcf_file = tmp_path / "high_risk.vcf"
        vcf_file.write_text(vcf_content)

        scanner = PHIScanner()
        result = scanner.scan_vcf_for_phi(vcf_file)

        assert result.has_phi is True
        assert result.risk_level == "high"

    def test_scan_medium_risk(self, tmp_path):
        vcf_content = (
            "##fileformat=VCFv4.2\n"
            "##source=/data/patients/sample1/data.vcf\n"
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
        )
        vcf_file = tmp_path / "medium_risk.vcf"
        vcf_file.write_text(vcf_content)

        scanner = PHIScanner()
        result = scanner.scan_vcf_for_phi(vcf_file)

        assert result.has_phi is True
        assert result.risk_level == "medium"

    def test_scan_gzipped_vcf(self, tmp_path):
        import gzip

        vcf_content = (
            "##fileformat=VCFv4.2\n"
            "##PatientID=12345\n"
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
        )
        vcf_file = tmp_path / "test.vcf.gz"
        with gzip.open(vcf_file, "wt") as f:
            f.write(vcf_content)

        scanner = PHIScanner()
        result = scanner.scan_vcf_for_phi(vcf_file)

        assert result.has_phi is True


class TestIntegration:
    def test_sanitizer_and_scanner_consistency(self, tmp_path):
        vcf_content = (
            "##fileformat=VCFv4.2\n"
            "##CommandLine=/home/jdoe/run.sh\n"
            "##source=Mayo Clinic\n"
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
        )
        vcf_file = tmp_path / "test.vcf"
        vcf_file.write_text(vcf_content)

        scanner = PHIScanner()
        scan_result = scanner.scan_vcf_for_phi(vcf_file)

        sanitizer = VCFHeaderSanitizer()
        with open(vcf_file) as f:
            header_lines = [line.rstrip() for line in f if line.startswith("#")]
        header_text = "\n".join(header_lines)
        sanitize_result = sanitizer.sanitize_header(header_text)

        assert scan_result.has_phi == sanitize_result.phi_detected
        assert len(scan_result.findings) == len(sanitize_result.removed_items)

    def test_roundtrip_sanitization(self, tmp_path):
        original_content = (
            "##fileformat=VCFv4.2\n"
            "##PatientID=12345\n"
            '##INFO=<ID=DP,Number=1,Type=Integer,Description="Depth">\n'
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
            "chr1\t100\t.\tA\tG\t30\tPASS\tDP=10\n"
        )

        sanitizer = VCFHeaderSanitizer()
        header_lines = [line for line in original_content.split("\n") if line.startswith("##")]
        header_text = "\n".join(header_lines)
        result = sanitizer.sanitize_header(header_text)

        scanner = PHIScanner()
        sanitized_header = "\n".join(result.sanitized_lines)

        sanitized_file = tmp_path / "sanitized.vcf"
        sanitized_file.write_text(
            sanitized_header + "\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
        )
        rescan = scanner.scan_vcf_for_phi(sanitized_file)

        assert rescan.has_phi is False
