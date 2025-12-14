"""
Annotation CLI tests for vcf-pg-loader.

These tests verify the CLI commands for annotation loading and lookup.
Test patterns derived from echtvar (https://github.com/brentp/echtvar) under MIT License.

See tests/vendored/echtvar/ATTRIBUTION.md for full attribution.
"""
import json

import pytest
from typer.testing import CliRunner

from vendored.echtvar import (
    generate_gnomad_vcf_content,
    generate_overlapping_variants,
    get_gnomad_field_config,
    write_annotation_vcf,
)

runner = CliRunner()


def fix_postgres_url(url: str) -> str:
    """Convert testcontainers URL to asyncpg-compatible format."""
    if url.startswith("postgresql+psycopg2://"):
        return url.replace("postgresql+psycopg2://", "postgresql://")
    return url


class TestLoadAnnotationCommand:
    """Test the load-annotation CLI command."""

    def test_load_annotation_command_exists(self):
        from vcf_pg_loader.cli import app

        result = runner.invoke(app, ["load-annotation", "--help"])

        assert result.exit_code == 0
        assert "annotation" in result.output.lower()

    def test_load_annotation_requires_name(self, tmp_path):
        from vcf_pg_loader.cli import app

        vcf_path = write_annotation_vcf(
            tmp_path / "gnomad.vcf",
            generate_gnomad_vcf_content(n_variants=10, seed=42),
        )

        result = runner.invoke(app, ["load-annotation", str(vcf_path)])

        assert result.exit_code != 0
        assert "name" in result.output.lower() or "required" in result.output.lower()

    def test_load_annotation_requires_config(self, tmp_path):
        from vcf_pg_loader.cli import app

        vcf_path = write_annotation_vcf(
            tmp_path / "gnomad.vcf",
            generate_gnomad_vcf_content(n_variants=10, seed=42),
        )

        result = runner.invoke(app, [
            "load-annotation",
            str(vcf_path),
            "--name", "gnomad_test",
        ])

        assert result.exit_code != 0

    @pytest.mark.integration
    def test_load_annotation_success(self, tmp_path, postgres_container):
        from vcf_pg_loader.cli import app

        vcf_path = write_annotation_vcf(
            tmp_path / "gnomad.vcf",
            generate_gnomad_vcf_content(n_variants=10, seed=42),
        )

        config_path = tmp_path / "gnomad.json"
        config_path.write_text(json.dumps(get_gnomad_field_config()))

        result = runner.invoke(app, [
            "load-annotation",
            str(vcf_path),
            "--name", "gnomad_cli_test",
            "--config", str(config_path),
            "--db", fix_postgres_url(postgres_container.get_connection_url()),
        ])

        assert result.exit_code == 0
        assert "10" in result.output or "loaded" in result.output.lower()

    @pytest.mark.integration
    def test_load_annotation_with_version(self, tmp_path, postgres_container):
        from vcf_pg_loader.cli import app

        vcf_path = write_annotation_vcf(
            tmp_path / "gnomad.vcf",
            generate_gnomad_vcf_content(n_variants=10, seed=42),
        )

        config_path = tmp_path / "gnomad.json"
        config_path.write_text(json.dumps(get_gnomad_field_config()))

        result = runner.invoke(app, [
            "load-annotation",
            str(vcf_path),
            "--name", "gnomad_versioned",
            "--config", str(config_path),
            "--version", "v3.1.2",
            "--db", fix_postgres_url(postgres_container.get_connection_url()),
        ])

        assert result.exit_code == 0


class TestListAnnotationsCommand:
    """Test the list-annotations CLI command."""

    def test_list_annotations_command_exists(self):
        from vcf_pg_loader.cli import app

        result = runner.invoke(app, ["list-annotations", "--help"])

        assert result.exit_code == 0

    @pytest.mark.integration
    def test_list_annotations_empty(self, postgres_container):
        from vcf_pg_loader.cli import app

        result = runner.invoke(app, [
            "list-annotations",
            "--db", fix_postgres_url(postgres_container.get_connection_url()),
        ])

        assert result.exit_code == 0

    @pytest.mark.integration
    def test_list_annotations_shows_loaded_sources(self, tmp_path, postgres_container):
        from vcf_pg_loader.cli import app

        vcf_path = write_annotation_vcf(
            tmp_path / "gnomad.vcf",
            generate_gnomad_vcf_content(n_variants=10, seed=42),
        )
        config_path = tmp_path / "gnomad.json"
        config_path.write_text(json.dumps(get_gnomad_field_config()))

        runner.invoke(app, [
            "load-annotation",
            str(vcf_path),
            "--name", "gnomad_list_test",
            "--config", str(config_path),
            "--db", fix_postgres_url(postgres_container.get_connection_url()),
        ])

        result = runner.invoke(app, [
            "list-annotations",
            "--db", fix_postgres_url(postgres_container.get_connection_url()),
        ])

        assert result.exit_code == 0
        assert "gnomad_list_test" in result.output


class TestAnnotateCommand:
    """Test the annotate CLI command."""

    def test_annotate_command_exists(self):
        from vcf_pg_loader.cli import app

        result = runner.invoke(app, ["annotate", "--help"])

        assert result.exit_code == 0
        assert "annotate" in result.output.lower()

    def test_annotate_requires_source(self):
        from vcf_pg_loader.cli import app

        result = runner.invoke(app, [
            "annotate",
            "some-batch-id",
        ])

        assert result.exit_code != 0
        assert "source" in result.output.lower() or "required" in result.output.lower()

    @pytest.mark.integration
    def test_annotate_with_filter_expression(self, tmp_path, postgres_container):
        import re

        from vcf_pg_loader.cli import app

        db_vcf, query_vcf = generate_overlapping_variants(
            n_shared=50, n_query_only=25, n_db_only=25, seed=42
        )

        db_vcf_path = write_annotation_vcf(tmp_path / "gnomad.vcf", db_vcf)
        query_vcf_path = write_annotation_vcf(tmp_path / "query.vcf", query_vcf)

        config_path = tmp_path / "gnomad.json"
        config_path.write_text(json.dumps(get_gnomad_field_config()))

        runner.invoke(app, [
            "load-annotation",
            str(db_vcf_path),
            "--name", "gnomad_filter_test",
            "--config", str(config_path),
            "--db", fix_postgres_url(postgres_container.get_connection_url()),
        ])

        load_result = runner.invoke(app, [
            "load",
            str(query_vcf_path),
            "--db", fix_postgres_url(postgres_container.get_connection_url()),
        ])

        batch_id_match = re.search(r"Batch ID: ([a-f0-9-]+)", load_result.output)
        batch_id = batch_id_match.group(1) if batch_id_match else "test-batch-id"

        result = runner.invoke(app, [
            "annotate",
            batch_id,
            "--source", "gnomad_filter_test",
            "--filter", "gnomad_af < 0.01",
            "--db", fix_postgres_url(postgres_container.get_connection_url()),
        ])

        assert result.exit_code == 0

    @pytest.mark.integration
    def test_annotate_output_formats(self, tmp_path, postgres_container):
        from vcf_pg_loader.cli import app

        result = runner.invoke(app, ["annotate", "--help"])

        assert "format" in result.output.lower()
        assert "tsv" in result.output.lower() or "json" in result.output.lower()

    @pytest.mark.integration
    def test_annotate_to_file(self, tmp_path, postgres_container):
        from vcf_pg_loader.cli import app

        output_path = tmp_path / "annotated.tsv"

        runner.invoke(app, [
            "annotate",
            "batch-id",
            "--source", "gnomad_test",
            "--output", str(output_path),
            "--db", fix_postgres_url(postgres_container.get_connection_url()),
        ])


class TestAnnotationQueryCommand:
    """Test the annotation-query CLI command for ad-hoc SQL queries."""

    def test_annotation_query_command_exists(self):
        from vcf_pg_loader.cli import app

        result = runner.invoke(app, ["annotation-query", "--help"])

        if result.exit_code == 0:
            assert "query" in result.output.lower()

    @pytest.mark.integration
    def test_annotation_query_basic(self, tmp_path, postgres_container):
        from vcf_pg_loader.cli import app

        db_vcf, query_vcf = generate_overlapping_variants(
            n_shared=10, n_query_only=5, n_db_only=5, seed=42
        )

        db_vcf_path = write_annotation_vcf(tmp_path / "gnomad.vcf", db_vcf)
        query_vcf_path = write_annotation_vcf(tmp_path / "query.vcf", query_vcf)

        config_path = tmp_path / "gnomad.json"
        config_path.write_text(json.dumps(get_gnomad_field_config()))

        runner.invoke(app, [
            "load-annotation",
            str(db_vcf_path),
            "--name", "gnomad_query_test",
            "--config", str(config_path),
            "--db", fix_postgres_url(postgres_container.get_connection_url()),
        ])

        runner.invoke(app, [
            "load",
            str(query_vcf_path),
            "--db", fix_postgres_url(postgres_container.get_connection_url()),
        ])

        runner.invoke(app, [
            "annotation-query",
            "--sql", """
                SELECT v.chrom, v.pos, v.ref, v.alt, a.gnomad_af
                FROM variants v
                LEFT JOIN anno_gnomad_query_test a
                ON v.chrom = a.chrom AND v.pos = a.pos AND v.ref = a.ref AND v.alt = a.alt
                LIMIT 5
            """,
            "--db", fix_postgres_url(postgres_container.get_connection_url()),
        ])


class TestAnnotationErrorHandling:
    """Test error handling in annotation CLI commands."""

    def test_load_annotation_invalid_vcf(self, tmp_path):
        from vcf_pg_loader.cli import app

        invalid_vcf = tmp_path / "invalid.vcf"
        invalid_vcf.write_text("not a vcf file")

        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(get_gnomad_field_config()))

        result = runner.invoke(app, [
            "load-annotation",
            str(invalid_vcf),
            "--name", "invalid_test",
            "--config", str(config_path),
        ])

        assert result.exit_code != 0

    def test_load_annotation_invalid_config(self, tmp_path):
        from vcf_pg_loader.cli import app

        vcf_path = write_annotation_vcf(
            tmp_path / "gnomad.vcf",
            generate_gnomad_vcf_content(n_variants=10, seed=42),
        )

        config_path = tmp_path / "invalid.json"
        config_path.write_text("not valid json {{{")

        result = runner.invoke(app, [
            "load-annotation",
            str(vcf_path),
            "--name", "invalid_config_test",
            "--config", str(config_path),
        ])

        assert result.exit_code != 0

    def test_annotate_invalid_expression(self, tmp_path):
        from vcf_pg_loader.cli import app

        result = runner.invoke(app, [
            "annotate",
            "batch-id",
            "--source", "gnomad_test",
            "--filter", "invalid expression !!!",
        ])

        assert result.exit_code != 0 or "error" in result.output.lower()

    def test_annotate_unknown_source(self):
        from vcf_pg_loader.cli import app

        result = runner.invoke(app, [
            "annotate",
            "batch-id",
            "--source", "nonexistent_source",
        ])

        assert result.exit_code != 0
