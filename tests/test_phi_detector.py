"""Tests for PHI detection patterns and detector."""

import pytest

from vcf_pg_loader.phi.detector import PHIDetection, PHIDetector, PHIScanReport
from vcf_pg_loader.phi.patterns import PHIPattern, PHIPatternRegistry


class TestPHIPattern:
    def test_create_pattern(self):
        import re

        pattern = PHIPattern(
            name="test_ssn",
            pattern=re.compile(r"\d{3}-\d{2}-\d{4}"),
            severity="critical",
            description="Test SSN pattern",
            false_positive_hints=["May be coordinates"],
        )
        assert pattern.name == "test_ssn"
        assert pattern.severity == "critical"
        assert pattern.false_positive_hints == ["May be coordinates"]

    def test_invalid_severity(self):
        import re

        with pytest.raises(ValueError, match="Invalid severity"):
            PHIPattern(
                name="test",
                pattern=re.compile(r"test"),
                severity="invalid",
                description="Test",
            )

    def test_from_dict(self):
        data = {
            "name": "custom_pattern",
            "pattern": r"\b[A-Z]{3}\d{6}\b",
            "severity": "high",
            "description": "Custom ID pattern",
            "case_insensitive": True,
            "false_positive_hints": ["May be gene ID"],
        }
        pattern = PHIPattern.from_dict(data)
        assert pattern.name == "custom_pattern"
        assert pattern.severity == "high"
        assert pattern.false_positive_hints == ["May be gene ID"]


class TestPHIPatternRegistry:
    def test_builtin_patterns_exist(self):
        registry = PHIPatternRegistry()
        assert len(registry.patterns) > 0
        pattern_names = {p.name for p in registry.patterns}
        assert "ssn" in pattern_names
        assert "mrn" in pattern_names
        assert "email" in pattern_names

    def test_add_pattern(self):
        import re

        registry = PHIPatternRegistry()
        initial_count = len(registry.patterns)

        pattern = PHIPattern(
            name="custom_test",
            pattern=re.compile(r"CUSTOM\d+"),
            severity="high",
            description="Custom test pattern",
        )
        registry.add_pattern(pattern)

        assert len(registry.patterns) == initial_count + 1
        assert registry.get_pattern("custom_test") is not None

    def test_remove_pattern(self):
        import re

        registry = PHIPatternRegistry()
        pattern = PHIPattern(
            name="to_remove",
            pattern=re.compile(r"REMOVE"),
            severity="low",
            description="To be removed",
        )
        registry.add_pattern(pattern)
        assert registry.get_pattern("to_remove") is not None

        result = registry.remove_pattern("to_remove")
        assert result is True
        assert registry.get_pattern("to_remove") is None

    def test_remove_nonexistent(self):
        registry = PHIPatternRegistry()
        result = registry.remove_pattern("does_not_exist")
        assert result is False

    def test_get_patterns_by_severity(self):
        registry = PHIPatternRegistry()
        critical_patterns = registry.get_patterns_by_severity("critical")
        assert len(critical_patterns) > 0
        assert all(p.severity == "critical" for p in critical_patterns)

    def test_load_custom_patterns(self, tmp_path):
        config_content = """
[[patterns]]
name = "custom_mrn"
pattern = "\\\\bCUST\\\\d{8}\\\\b"
severity = "high"
description = "Custom MRN format"
false_positive_hints = ["May be order ID"]

[[patterns]]
name = "internal_id"
pattern = "\\\\bINT-\\\\d{6}\\\\b"
severity = "medium"
description = "Internal ID"
"""
        config_file = tmp_path / "phi_patterns.toml"
        config_file.write_text(config_content)

        registry = PHIPatternRegistry()
        initial_count = len(registry.patterns)
        count = registry.load_custom_patterns(config_file)

        assert count == 2
        assert len(registry.patterns) == initial_count + 2
        assert registry.get_pattern("custom_mrn") is not None
        assert registry.get_pattern("internal_id") is not None

    def test_load_missing_config(self, tmp_path):
        registry = PHIPatternRegistry()
        with pytest.raises(FileNotFoundError):
            registry.load_custom_patterns(tmp_path / "nonexistent.toml")

    def test_clear_custom_patterns(self):
        import re

        registry = PHIPatternRegistry()
        builtin_count = len(PHIPatternRegistry.BUILTIN_PATTERNS)

        registry.add_pattern(
            PHIPattern(
                name="custom1",
                pattern=re.compile(r"CUSTOM1"),
                severity="low",
                description="Custom 1",
            )
        )
        registry.add_pattern(
            PHIPattern(
                name="custom2",
                pattern=re.compile(r"CUSTOM2"),
                severity="low",
                description="Custom 2",
            )
        )

        assert len(registry.patterns) == builtin_count + 2

        registry.clear_custom_patterns()
        assert len(registry.patterns) == builtin_count


class TestPHIDetection:
    def test_masked_value_short(self):
        detection = PHIDetection(
            pattern_name="test",
            matched_value="123",
            location="INFO",
            context="test 123 test",
            severity="high",
        )
        assert detection.masked_value == "***"

    def test_masked_value_long(self):
        detection = PHIDetection(
            pattern_name="test",
            matched_value="123-45-6789",
            location="INFO",
            context="SSN: 123-45-6789",
            severity="critical",
        )
        assert detection.masked_value == "12*******89"

    def test_masked_value_medium(self):
        detection = PHIDetection(
            pattern_name="test",
            matched_value="12345",
            location="INFO",
            context="ID: 12345",
            severity="high",
        )
        assert detection.masked_value == "12*45"


class TestPHIDetector:
    def test_scan_value_ssn(self):
        detector = PHIDetector()
        detections = detector.scan_value("SSN: 123-45-6789", "INFO")
        assert len(detections) >= 1
        ssn_detection = next((d for d in detections if d.pattern_name == "ssn"), None)
        assert ssn_detection is not None
        assert ssn_detection.severity == "critical"

    def test_scan_value_email(self):
        detector = PHIDetector()
        detections = detector.scan_value("Contact: patient@hospital.org", "HEADER")
        email_detection = next((d for d in detections if d.pattern_name == "email"), None)
        assert email_detection is not None
        assert email_detection.severity == "high"

    def test_scan_value_mrn(self):
        detector = PHIDetector()
        detections = detector.scan_value("MRN:12345678", "SAMPLE")
        mrn_detection = next((d for d in detections if d.pattern_name == "mrn"), None)
        assert mrn_detection is not None
        assert mrn_detection.severity == "critical"

    def test_scan_value_no_phi(self):
        detector = PHIDetector()
        detections = detector.scan_value("CHROM\tPOS\tID\tREF\tALT", "HEADER")
        assert all(d.pattern_name not in ("ssn", "mrn", "email") for d in detections)

    def test_scan_value_accumulates(self):
        detector = PHIDetector()
        detector.scan_value("123-45-6789", "INFO/1")
        detector.scan_value("987-65-4321", "INFO/2")
        assert len(detector.detections) >= 2

    def test_clear_detections(self):
        detector = PHIDetector()
        detector.scan_value("123-45-6789", "INFO")
        assert len(detector.detections) > 0
        detector.clear_detections()
        assert len(detector.detections) == 0

    def test_context_truncation(self):
        detector = PHIDetector()
        long_text = "X" * 50 + " 123-45-6789 " + "Y" * 50
        detections = detector.scan_value(long_text, "INFO")
        ssn_detection = next((d for d in detections if d.pattern_name == "ssn"), None)
        assert ssn_detection is not None
        assert ssn_detection.matched_value == "123-45-6789"
        assert "..." in ssn_detection.context


class TestPHIScanReport:
    def test_has_phi_true(self):
        detection = PHIDetection(
            pattern_name="ssn",
            matched_value="123-45-6789",
            location="INFO",
            context="test",
            severity="critical",
        )
        report = PHIScanReport(
            detections=[detection],
            records_scanned=100,
            records_total=100,
            sample_rate=1.0,
        )
        assert report.has_phi is True

    def test_has_phi_false(self):
        report = PHIScanReport(
            detections=[],
            records_scanned=100,
            records_total=100,
            sample_rate=1.0,
        )
        assert report.has_phi is False

    def test_summary(self):
        detections = [
            PHIDetection("ssn", "1", "A", "ctx", "critical"),
            PHIDetection("ssn", "2", "B", "ctx", "critical"),
            PHIDetection("email", "3", "C", "ctx", "high"),
        ]
        report = PHIScanReport(
            detections=detections,
            records_scanned=10,
            records_total=10,
            sample_rate=1.0,
        )
        assert report.summary == {"ssn": 2, "email": 1}

    def test_severity_summary(self):
        detections = [
            PHIDetection("ssn", "1", "A", "ctx", "critical"),
            PHIDetection("mrn", "2", "B", "ctx", "critical"),
            PHIDetection("email", "3", "C", "ctx", "high"),
        ]
        report = PHIScanReport(
            detections=detections,
            records_scanned=10,
            records_total=10,
            sample_rate=1.0,
        )
        assert report.severity_summary == {"critical": 2, "high": 1}

    def test_risk_level_critical(self):
        detections = [
            PHIDetection("ssn", "1", "A", "ctx", "critical"),
        ]
        report = PHIScanReport(
            detections=detections, records_scanned=1, records_total=1, sample_rate=1.0
        )
        assert report.risk_level == "critical"

    def test_risk_level_high(self):
        detections = [
            PHIDetection("email", "1", "A", "ctx", "high"),
        ]
        report = PHIScanReport(
            detections=detections, records_scanned=1, records_total=1, sample_rate=1.0
        )
        assert report.risk_level == "high"

    def test_risk_level_none(self):
        report = PHIScanReport(detections=[], records_scanned=1, records_total=1, sample_rate=1.0)
        assert report.risk_level == "none"


class TestPHIDetectorVCFStream:
    @pytest.fixture
    def vcf_with_phi(self, tmp_path):
        vcf_content = """##fileformat=VCFv4.2
##INFO=<ID=DP,Number=1,Type=Integer,Description="Depth">
##INFO=<ID=PATIENT,Number=1,Type=String,Description="Patient_ID: 123-45-6789">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tJohnDoe_2024
chr1\t100\t.\tA\tG\t30\tPASS\tDP=50;MRN:9876543\tGT\t0/1
chr1\t200\t.\tC\tT\t40\tPASS\tDP=60\tGT\t1/1
"""
        vcf_file = tmp_path / "test_phi.vcf"
        vcf_file.write_text(vcf_content)
        return vcf_file

    @pytest.fixture
    def vcf_clean(self, tmp_path):
        vcf_content = """##fileformat=VCFv4.2
##INFO=<ID=DP,Number=1,Type=Integer,Description="Read Depth">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE001
chr1\t100\trs123\tA\tG\t30\tPASS\tDP=50\tGT\t0/1
chr1\t200\t.\tC\tT\t40\tPASS\tDP=60\tGT\t1/1
"""
        vcf_file = tmp_path / "test_clean.vcf"
        vcf_file.write_text(vcf_content)
        return vcf_file

    def test_scan_vcf_with_phi(self, vcf_with_phi):
        detector = PHIDetector()
        report = detector.scan_vcf_stream(vcf_with_phi)

        assert report.has_phi
        assert report.records_scanned == 2
        assert report.records_total == 2
        assert "ssn" in report.summary or "mrn" in report.summary

    def test_scan_vcf_clean(self, vcf_clean):
        detector = PHIDetector()
        report = detector.scan_vcf_stream(vcf_clean)

        assert report.records_scanned == 2
        assert report.records_total == 2

    def test_scan_vcf_with_sampling(self, vcf_with_phi):
        detector = PHIDetector()
        report = detector.scan_vcf_stream(vcf_with_phi, sample_rate=0.0)
        assert report.records_scanned == 0
        assert report.records_total == 2

    def test_scan_vcf_max_records(self, tmp_path):
        lines = ["##fileformat=VCFv4.2\n"]
        lines.append("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n")
        for i in range(100):
            lines.append(f"chr1\t{i*100}\t.\tA\tG\t30\tPASS\tDP=50\n")

        vcf_file = tmp_path / "large.vcf"
        vcf_file.write_text("".join(lines))

        detector = PHIDetector()
        report = detector.scan_vcf_stream(vcf_file, max_records=10)
        assert report.records_scanned == 10
        assert report.records_total == 100

    def test_scan_vcf_no_headers(self, vcf_with_phi):
        detector = PHIDetector()
        report_with = detector.scan_vcf_stream(vcf_with_phi, scan_headers=True)
        detector.clear_detections()
        report_without = detector.scan_vcf_stream(vcf_with_phi, scan_headers=False)

        header_detections = [d for d in report_with.detections if "HEADER" in d.location]
        assert len(header_detections) > 0 or len(report_with.detections) >= len(
            report_without.detections
        )


class TestPHIDetectorMasking:
    def test_mask_phi_single(self):
        detector = PHIDetector()
        detector.scan_value("SSN: 123-45-6789", "INFO")
        masked = detector.mask_phi("The SSN is 123-45-6789 for this patient")
        assert "123-45-6789" not in masked
        assert "12*******89" in masked

    def test_mask_phi_multiple(self):
        detector = PHIDetector()
        detector.scan_value("123-45-6789 and test@example.com", "INFO")
        text = "Contact: test@example.com, SSN: 123-45-6789"
        masked = detector.mask_phi(text)
        assert "123-45-6789" not in masked
        assert "test@example.com" not in masked
