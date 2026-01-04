"""Tests for imputation quality metrics extraction.

TDD tests for post-imputation VCF parsing including:
- Auto-detection of imputation source (Minimac4, Beagle, IMPUTE2)
- Extraction of R² / DR2 / INFO quality scores
- IMPUTED/TYPED flag parsing
- --min-info-score filtering behavior

Field mapping by imputation source:
- Minimac4: R2 (quality), IMPUTED/TYPED flags
- Beagle 5.x: DR2 (quality), IMP flag
- IMPUTE2: INFO (quality), derived from presence of INFO
"""

import pytest


class TestImputationSourceDetection:
    """Test auto-detection of imputation source from VCF headers."""

    def test_detect_minimac4_from_header(self):
        from vcf_pg_loader.parsers.imputation import ImputationSource, detect_imputation_source

        header = """##fileformat=VCFv4.2
##source=Minimac4
##INFO=<ID=R2,Number=1,Type=Float,Description="Estimated Imputation Accuracy">
##INFO=<ID=IMPUTED,Number=0,Type=Flag,Description="Imputed marker">
##INFO=<ID=TYPED,Number=0,Type=Flag,Description="Typed marker">
"""
        source = detect_imputation_source(header)
        assert source == ImputationSource.MINIMAC4

    def test_detect_beagle_from_header(self):
        from vcf_pg_loader.parsers.imputation import ImputationSource, detect_imputation_source

        header = """##fileformat=VCFv4.2
##source=beagle.27Jan18.7e1.jar
##INFO=<ID=DR2,Number=1,Type=Float,Description="Dosage R-Squared: estimated squared correlation between estimated REF dose and true REF dose">
##INFO=<ID=IMP,Number=0,Type=Flag,Description="Imputed marker">
"""
        source = detect_imputation_source(header)
        assert source == ImputationSource.BEAGLE

    def test_detect_impute2_from_header(self):
        from vcf_pg_loader.parsers.imputation import ImputationSource, detect_imputation_source

        header = """##fileformat=VCFv4.2
##source=IMPUTE2
##INFO=<ID=INFO,Number=1,Type=Float,Description="IMPUTE2 info score">
"""
        source = detect_imputation_source(header)
        assert source == ImputationSource.IMPUTE2

    def test_detect_michigan_imputation_server(self):
        from vcf_pg_loader.parsers.imputation import ImputationSource, detect_imputation_source

        header = """##fileformat=VCFv4.2
##INFO=<ID=R2,Number=1,Type=Float,Description="Estimated Imputation Accuracy (R-squared)">
##INFO=<ID=IMPUTED,Number=0,Type=Flag,Description="Imputed marker">
##source=Michigan Imputation Server
"""
        source = detect_imputation_source(header)
        assert source == ImputationSource.MINIMAC4

    def test_detect_unknown_source(self):
        from vcf_pg_loader.parsers.imputation import ImputationSource, detect_imputation_source

        header = """##fileformat=VCFv4.2
##source=SomeUnknownTool
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total Depth">
"""
        source = detect_imputation_source(header)
        assert source == ImputationSource.UNKNOWN

    def test_detect_from_r2_field_without_source(self):
        from vcf_pg_loader.parsers.imputation import ImputationSource, detect_imputation_source

        header = """##fileformat=VCFv4.2
##INFO=<ID=R2,Number=1,Type=Float,Description="Estimated Imputation Accuracy">
"""
        source = detect_imputation_source(header)
        assert source == ImputationSource.MINIMAC4

    def test_detect_from_dr2_field_without_source(self):
        from vcf_pg_loader.parsers.imputation import ImputationSource, detect_imputation_source

        header = """##fileformat=VCFv4.2
##INFO=<ID=DR2,Number=1,Type=Float,Description="Dosage R-squared">
"""
        source = detect_imputation_source(header)
        assert source == ImputationSource.BEAGLE


class TestImputationMetricsExtraction:
    """Test extraction of imputation quality metrics from INFO fields."""

    def test_extract_minimac4_r2(self):
        from vcf_pg_loader.parsers.imputation import (
            ImputationSource,
            extract_imputation_metrics,
        )

        info = {"R2": 0.95, "IMPUTED": True}
        metrics = extract_imputation_metrics(info, ImputationSource.MINIMAC4)

        assert metrics.info_score == pytest.approx(0.95)
        assert metrics.imputation_r2 == pytest.approx(0.95)
        assert metrics.is_imputed is True
        assert metrics.is_typed is False

    def test_extract_minimac4_typed(self):
        from vcf_pg_loader.parsers.imputation import (
            ImputationSource,
            extract_imputation_metrics,
        )

        info = {"R2": 1.0, "TYPED": True}
        metrics = extract_imputation_metrics(info, ImputationSource.MINIMAC4)

        assert metrics.info_score == pytest.approx(1.0)
        assert metrics.is_imputed is False
        assert metrics.is_typed is True

    def test_extract_beagle_dr2(self):
        from vcf_pg_loader.parsers.imputation import (
            ImputationSource,
            extract_imputation_metrics,
        )

        info = {"DR2": 0.87, "IMP": True}
        metrics = extract_imputation_metrics(info, ImputationSource.BEAGLE)

        assert metrics.info_score == pytest.approx(0.87)
        assert metrics.imputation_r2 == pytest.approx(0.87)
        assert metrics.is_imputed is True

    def test_extract_beagle_typed_no_imp(self):
        from vcf_pg_loader.parsers.imputation import (
            ImputationSource,
            extract_imputation_metrics,
        )

        info = {"DR2": 1.0}
        metrics = extract_imputation_metrics(info, ImputationSource.BEAGLE)

        assert metrics.info_score == pytest.approx(1.0)
        assert metrics.is_imputed is False
        assert metrics.is_typed is True

    def test_extract_impute2_info(self):
        from vcf_pg_loader.parsers.imputation import (
            ImputationSource,
            extract_imputation_metrics,
        )

        info = {"INFO": 0.92}
        metrics = extract_imputation_metrics(info, ImputationSource.IMPUTE2)

        assert metrics.info_score == pytest.approx(0.92)
        assert metrics.imputation_r2 == pytest.approx(0.92)
        assert metrics.is_imputed is True

    def test_extract_missing_score_returns_none(self):
        from vcf_pg_loader.parsers.imputation import (
            ImputationSource,
            extract_imputation_metrics,
        )

        info = {"DP": 30}
        metrics = extract_imputation_metrics(info, ImputationSource.MINIMAC4)

        assert metrics.info_score is None
        assert metrics.imputation_r2 is None
        assert metrics.is_imputed is False
        assert metrics.is_typed is False

    def test_extract_unknown_source(self):
        from vcf_pg_loader.parsers.imputation import (
            ImputationSource,
            extract_imputation_metrics,
        )

        info = {"R2": 0.8}
        metrics = extract_imputation_metrics(info, ImputationSource.UNKNOWN)

        assert metrics.info_score is None

    def test_extract_auto_detection(self):
        from vcf_pg_loader.parsers.imputation import (
            ImputationSource,
            extract_imputation_metrics,
        )

        info = {"R2": 0.75}
        metrics = extract_imputation_metrics(info, ImputationSource.AUTO)

        assert metrics.info_score == pytest.approx(0.75)
        assert metrics.imputation_r2 == pytest.approx(0.75)


class TestImputationMetricsEdgeCases:
    """Test edge cases in imputation metrics extraction."""

    def test_r2_as_string(self):
        from vcf_pg_loader.parsers.imputation import (
            ImputationSource,
            extract_imputation_metrics,
        )

        info = {"R2": "0.85"}
        metrics = extract_imputation_metrics(info, ImputationSource.MINIMAC4)

        assert metrics.info_score == pytest.approx(0.85)

    def test_r2_invalid_string(self):
        from vcf_pg_loader.parsers.imputation import (
            ImputationSource,
            extract_imputation_metrics,
        )

        info = {"R2": "invalid"}
        metrics = extract_imputation_metrics(info, ImputationSource.MINIMAC4)

        assert metrics.info_score is None

    def test_r2_as_list_takes_first(self):
        from vcf_pg_loader.parsers.imputation import (
            ImputationSource,
            extract_imputation_metrics,
        )

        info = {"R2": [0.9, 0.8]}
        metrics = extract_imputation_metrics(info, ImputationSource.MINIMAC4)

        assert metrics.info_score == pytest.approx(0.9)

    def test_imputed_flag_as_int(self):
        from vcf_pg_loader.parsers.imputation import (
            ImputationSource,
            extract_imputation_metrics,
        )

        info = {"R2": 0.8, "IMPUTED": 1}
        metrics = extract_imputation_metrics(info, ImputationSource.MINIMAC4)

        assert metrics.is_imputed is True

    def test_empty_info_dict(self):
        from vcf_pg_loader.parsers.imputation import (
            ImputationSource,
            extract_imputation_metrics,
        )

        info = {}
        metrics = extract_imputation_metrics(info, ImputationSource.MINIMAC4)

        assert metrics.info_score is None
        assert metrics.is_imputed is False

    def test_mixed_typed_and_imputed(self):
        from vcf_pg_loader.parsers.imputation import (
            ImputationSource,
            extract_imputation_metrics,
        )

        info = {"R2": 0.99, "TYPED": True, "IMPUTED": True}
        metrics = extract_imputation_metrics(info, ImputationSource.MINIMAC4)

        assert metrics.is_typed is True
        assert metrics.is_imputed is True


class TestImputationConfig:
    """Test ImputationConfig dataclass."""

    def test_config_defaults(self):
        from vcf_pg_loader.parsers.imputation import ImputationConfig

        config = ImputationConfig()

        assert config.source == "auto"
        assert config.min_info_score is None

    def test_config_with_values(self):
        from vcf_pg_loader.parsers.imputation import ImputationConfig

        config = ImputationConfig(source="minimac4", min_info_score=0.8)

        assert config.source == "minimac4"
        assert config.min_info_score == 0.8

    def test_config_get_source_enum(self):
        from vcf_pg_loader.parsers.imputation import ImputationConfig, ImputationSource

        config = ImputationConfig(source="beagle")
        assert config.get_source_enum() == ImputationSource.BEAGLE

        config = ImputationConfig(source="minimac4")
        assert config.get_source_enum() == ImputationSource.MINIMAC4

        config = ImputationConfig(source="impute2")
        assert config.get_source_enum() == ImputationSource.IMPUTE2

        config = ImputationConfig(source="auto")
        assert config.get_source_enum() == ImputationSource.AUTO

    def test_should_filter_variant(self):
        from vcf_pg_loader.parsers.imputation import ImputationConfig

        config = ImputationConfig(min_info_score=0.8)

        assert config.should_filter_variant(0.9) is False
        assert config.should_filter_variant(0.8) is False
        assert config.should_filter_variant(0.79) is True
        assert config.should_filter_variant(None) is False

    def test_should_filter_no_threshold(self):
        from vcf_pg_loader.parsers.imputation import ImputationConfig

        config = ImputationConfig()

        assert config.should_filter_variant(0.1) is False
        assert config.should_filter_variant(None) is False


class TestImputationHeaderParser:
    """Test header parsing for imputation detection."""

    def test_parse_header_with_imputation_fields(self):
        from vcf_pg_loader.parsers.imputation import parse_imputation_header

        header = """##fileformat=VCFv4.2
##source=Minimac4
##INFO=<ID=R2,Number=1,Type=Float,Description="Estimated Imputation Accuracy">
##INFO=<ID=IMPUTED,Number=0,Type=Flag,Description="Imputed marker">
##INFO=<ID=TYPED,Number=0,Type=Flag,Description="Typed marker">
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total Depth">
"""
        info = parse_imputation_header(header)

        assert info.has_r2 is True
        assert info.has_dr2 is False
        assert info.has_info_score is False
        assert info.has_imputed_flag is True
        assert info.has_typed_flag is True
        assert info.detected_source is not None

    def test_parse_header_beagle(self):
        from vcf_pg_loader.parsers.imputation import parse_imputation_header

        header = """##fileformat=VCFv4.2
##INFO=<ID=DR2,Number=1,Type=Float,Description="Dosage R-squared">
##INFO=<ID=IMP,Number=0,Type=Flag,Description="Imputed marker">
"""
        info = parse_imputation_header(header)

        assert info.has_r2 is False
        assert info.has_dr2 is True
        assert info.has_imp_flag is True


class TestMinInfoScoreFiltering:
    """Test --min-info-score filtering behavior."""

    def test_filter_variants_below_threshold(self):
        from vcf_pg_loader.parsers.imputation import filter_by_info_score

        variants = [
            {"info_score": 0.9},
            {"info_score": 0.5},
            {"info_score": 0.8},
            {"info_score": None},
            {"info_score": 0.79},
        ]

        filtered, skipped = filter_by_info_score(variants, min_score=0.8)

        assert len(filtered) == 3
        assert skipped == 2
        assert all(v["info_score"] is None or v["info_score"] >= 0.8 for v in filtered)

    def test_filter_no_threshold(self):
        from vcf_pg_loader.parsers.imputation import filter_by_info_score

        variants = [{"info_score": 0.1}, {"info_score": 0.9}]

        filtered, skipped = filter_by_info_score(variants, min_score=None)

        assert len(filtered) == 2
        assert skipped == 0

    def test_filter_empty_list(self):
        from vcf_pg_loader.parsers.imputation import filter_by_info_score

        filtered, skipped = filter_by_info_score([], min_score=0.8)

        assert len(filtered) == 0
        assert skipped == 0

    def test_filter_all_below_threshold(self):
        from vcf_pg_loader.parsers.imputation import filter_by_info_score

        variants = [
            {"info_score": 0.3},
            {"info_score": 0.4},
            {"info_score": 0.5},
        ]

        filtered, skipped = filter_by_info_score(variants, min_score=0.6)

        assert len(filtered) == 0
        assert skipped == 3


class TestImputationSourceEnum:
    """Test ImputationSource enum."""

    def test_source_values(self):
        from vcf_pg_loader.parsers.imputation import ImputationSource

        assert ImputationSource.MINIMAC4.value == "minimac4"
        assert ImputationSource.BEAGLE.value == "beagle"
        assert ImputationSource.IMPUTE2.value == "impute2"
        assert ImputationSource.AUTO.value == "auto"
        assert ImputationSource.UNKNOWN.value == "unknown"

    def test_source_from_string(self):
        from vcf_pg_loader.parsers.imputation import ImputationSource

        assert ImputationSource.from_string("minimac4") == ImputationSource.MINIMAC4
        assert ImputationSource.from_string("MINIMAC4") == ImputationSource.MINIMAC4
        assert ImputationSource.from_string("beagle") == ImputationSource.BEAGLE
        assert ImputationSource.from_string("impute2") == ImputationSource.IMPUTE2
        assert ImputationSource.from_string("auto") == ImputationSource.AUTO
        assert ImputationSource.from_string("invalid") == ImputationSource.UNKNOWN


class TestImputationMetricsDataclass:
    """Test ImputationMetrics dataclass."""

    def test_metrics_defaults(self):
        from vcf_pg_loader.parsers.imputation import ImputationMetrics

        metrics = ImputationMetrics()

        assert metrics.info_score is None
        assert metrics.imputation_r2 is None
        assert metrics.is_imputed is False
        assert metrics.is_typed is False
        assert metrics.source is None

    def test_metrics_with_values(self):
        from vcf_pg_loader.parsers.imputation import ImputationMetrics

        metrics = ImputationMetrics(
            info_score=0.95,
            imputation_r2=0.95,
            is_imputed=True,
            is_typed=False,
            source="minimac4",
        )

        assert metrics.info_score == 0.95
        assert metrics.imputation_r2 == 0.95
        assert metrics.is_imputed is True
        assert metrics.is_typed is False
        assert metrics.source == "minimac4"
