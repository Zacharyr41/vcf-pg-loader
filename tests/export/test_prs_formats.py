"""TDD tests for PRS tool export formats.

Tests for exporting GWAS summary statistics to common PRS tool input formats:
- PLINK 2.0 --score format
- PRS-CS format
- LDpred2 bigsnpr format
- PRSice-2 format
"""

import tempfile
import uuid
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture
async def pg_pool():
    """Create a test PostgreSQL connection pool using testcontainers."""
    import asyncpg
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:15") as postgres:
        pool = await asyncpg.create_pool(
            host=postgres.get_container_host_ip(),
            port=int(postgres.get_exposed_port(5432)),
            user=postgres.username,
            password=postgres.password,
            database=postgres.dbname,
            min_size=1,
            max_size=5,
        )
        yield pool
        await pool.close()


@pytest.fixture
async def db_with_gwas_data(pg_pool):
    """Set up database with GWAS summary statistics for export testing."""
    from vcf_pg_loader.gwas.schema import GWASSchemaManager
    from vcf_pg_loader.references.schema import ReferenceSchemaManager
    from vcf_pg_loader.schema import SchemaManager

    async with pg_pool.acquire() as conn:
        schema_mgr = SchemaManager(human_genome=True)
        await schema_mgr.create_schema(conn, skip_encryption=True, skip_emergency=True)
        await schema_mgr.create_indexes(conn)

        gwas_mgr = GWASSchemaManager()
        await gwas_mgr.create_gwas_schema(conn)

        ref_mgr = ReferenceSchemaManager()
        await ref_mgr.create_reference_panels_table(conn)

        batch_id = uuid.uuid4()

        test_variants = [
            ("chr1", 100, "A", "G", "rs100", batch_id, True, 0.95, 0.15),
            ("chr1", 200, "C", "T", "rs200", batch_id, True, 0.90, 0.25),
            ("chr1", 300, "G", "A", "rs300", batch_id, True, 0.85, 0.10),
            ("chr2", 100, "T", "C", "rs400", batch_id, False, 0.95, 0.20),
            ("chr2", 200, "A", "G", "rs500", batch_id, True, 0.50, 0.05),
            ("chr2", 300, "C", "T", "rs600", batch_id, True, 0.95, 0.30),
        ]

        for chrom, pos, ref, alt, rs_id, bid, in_hm3, info, maf in test_variants:
            await conn.execute(
                """
                INSERT INTO variants (
                    chrom, pos, pos_range, ref, alt, rs_id, load_batch_id,
                    in_hapmap3, info_score, maf
                ) VALUES ($1, $2::bigint, int8range($2::bigint, $2::bigint+1), $3, $4, $5, $6, $7, $8, $9)
                """,
                chrom,
                pos,
                ref,
                alt,
                rs_id,
                bid,
                in_hm3,
                info,
                maf,
            )

        for rs_id, chrom, pos in [
            ("rs100", "1", 100),
            ("rs200", "1", 200),
            ("rs300", "1", 300),
            ("rs600", "2", 300),
        ]:
            await conn.execute(
                """
                INSERT INTO reference_panels (panel_name, rsid, chrom, position, a1, a2)
                VALUES ('hapmap3_grch38', $1, $2, $3, 'A', 'G')
                """,
                rs_id,
                chrom,
                pos,
            )

        study_id = await conn.fetchval(
            """
            INSERT INTO studies (
                study_accession, trait_name, genome_build, sample_size, n_cases, n_controls
            ) VALUES ('GCST001', 'Test Trait', 'GRCh38', 100000, 40000, 60000)
            RETURNING study_id
            """
        )

        variant_rows = await conn.fetch(
            "SELECT variant_id, chrom, pos, ref, alt FROM variants ORDER BY chrom, pos"
        )

        gwas_data = [
            (0.05, 0.01, 1e-6, "G", True),
            (0.03, 0.008, 1e-4, "T", True),
            (-0.02, 0.015, 0.01, "G", False),
            (0.04, 0.012, 1e-3, "C", True),
            (0.06, 0.02, 5e-8, "G", True),
            (-0.01, 0.005, 0.5, "T", True),
        ]

        for i, row in enumerate(variant_rows):
            beta, se, pval, ea, is_alt = gwas_data[i]
            await conn.execute(
                """
                INSERT INTO gwas_summary_stats (
                    variant_id, study_id, effect_allele, other_allele,
                    beta, standard_error, p_value, is_effect_allele_alt
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                row["variant_id"],
                study_id,
                ea,
                row["ref"] if is_alt else row["alt"],
                beta,
                se,
                pval,
                is_alt,
            )

    yield pg_pool, study_id


class TestPlinkScoreExport:
    """Test PLINK 2.0 --score format export."""

    async def test_export_basic_format(self, db_with_gwas_data):
        from vcf_pg_loader.export.prs_formats import export_plink_score

        pool, study_id = db_with_gwas_data

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            output_path = Path(f.name)

        async with pool.acquire() as conn:
            count = await export_plink_score(conn, study_id, output_path)

        assert count == 6
        assert output_path.exists()

        content = output_path.read_text()
        lines = content.strip().split("\n")

        assert lines[0] == "SNP\tA1\tBETA"
        assert len(lines) == 7

    async def test_export_correct_alleles(self, db_with_gwas_data):
        from vcf_pg_loader.export.prs_formats import export_plink_score

        pool, study_id = db_with_gwas_data

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            output_path = Path(f.name)

        async with pool.acquire() as conn:
            await export_plink_score(conn, study_id, output_path)

        content = output_path.read_text()
        lines = content.strip().split("\n")[1:]

        for line in lines:
            parts = line.split("\t")
            assert len(parts) == 3
            snp, a1, beta = parts
            assert snp.startswith("rs")
            assert a1 in ["A", "C", "G", "T"]
            float(beta)

    async def test_export_hapmap3_filter(self, db_with_gwas_data):
        from vcf_pg_loader.export.prs_formats import VariantFilter, export_plink_score

        pool, study_id = db_with_gwas_data

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            output_path = Path(f.name)

        variant_filter = VariantFilter(hapmap3_only=True)

        async with pool.acquire() as conn:
            count = await export_plink_score(conn, study_id, output_path, variant_filter)

        assert count == 5

    async def test_export_info_score_filter(self, db_with_gwas_data):
        from vcf_pg_loader.export.prs_formats import VariantFilter, export_plink_score

        pool, study_id = db_with_gwas_data

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            output_path = Path(f.name)

        variant_filter = VariantFilter(min_info=0.9)

        async with pool.acquire() as conn:
            count = await export_plink_score(conn, study_id, output_path, variant_filter)

        assert count == 4

    async def test_export_maf_filter(self, db_with_gwas_data):
        from vcf_pg_loader.export.prs_formats import VariantFilter, export_plink_score

        pool, study_id = db_with_gwas_data

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            output_path = Path(f.name)

        variant_filter = VariantFilter(min_maf=0.15)

        async with pool.acquire() as conn:
            count = await export_plink_score(conn, study_id, output_path, variant_filter)

        assert count == 4


class TestPRSCSExport:
    """Test PRS-CS format export."""

    async def test_export_with_pvalue(self, db_with_gwas_data):
        from vcf_pg_loader.export.prs_formats import export_prs_cs

        pool, study_id = db_with_gwas_data

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            output_path = Path(f.name)

        async with pool.acquire() as conn:
            count = await export_prs_cs(conn, study_id, output_path, use_se=False)

        assert count == 6

        content = output_path.read_text()
        lines = content.strip().split("\n")

        assert lines[0] == "SNP\tA1\tA2\tBETA\tP"

    async def test_export_with_se(self, db_with_gwas_data):
        from vcf_pg_loader.export.prs_formats import export_prs_cs

        pool, study_id = db_with_gwas_data

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            output_path = Path(f.name)

        async with pool.acquire() as conn:
            count = await export_prs_cs(conn, study_id, output_path, use_se=True)

        assert count == 6

        content = output_path.read_text()
        lines = content.strip().split("\n")

        assert lines[0] == "SNP\tA1\tA2\tBETA\tSE"

    async def test_export_includes_both_alleles(self, db_with_gwas_data):
        from vcf_pg_loader.export.prs_formats import export_prs_cs

        pool, study_id = db_with_gwas_data

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            output_path = Path(f.name)

        async with pool.acquire() as conn:
            await export_prs_cs(conn, study_id, output_path, use_se=True)

        content = output_path.read_text()
        lines = content.strip().split("\n")[1:]

        for line in lines:
            parts = line.split("\t")
            assert len(parts) == 5
            snp, a1, a2, beta, se_or_p = parts
            assert a1 != a2
            assert a1 in ["A", "C", "G", "T"]
            assert a2 in ["A", "C", "G", "T"]

    async def test_export_with_filter(self, db_with_gwas_data):
        from vcf_pg_loader.export.prs_formats import VariantFilter, export_prs_cs

        pool, study_id = db_with_gwas_data

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            output_path = Path(f.name)

        variant_filter = VariantFilter(hapmap3_only=True, min_info=0.85)

        async with pool.acquire() as conn:
            count = await export_prs_cs(
                conn, study_id, output_path, use_se=True, variant_filter=variant_filter
            )

        assert count == 4


class TestLDpred2Export:
    """Test LDpred2 bigsnpr format export."""

    async def test_export_basic_format(self, db_with_gwas_data):
        from vcf_pg_loader.export.prs_formats import export_ldpred2

        pool, study_id = db_with_gwas_data

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            output_path = Path(f.name)

        async with pool.acquire() as conn:
            count = await export_ldpred2(conn, study_id, output_path)

        assert count == 6

        content = output_path.read_text()
        lines = content.strip().split("\n")

        assert lines[0] == "chr\tpos\ta0\ta1\tbeta\tbeta_se\tn_eff"

    async def test_export_neff_computation(self, db_with_gwas_data):
        from vcf_pg_loader.export.prs_formats import export_ldpred2

        pool, study_id = db_with_gwas_data

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            output_path = Path(f.name)

        async with pool.acquire() as conn:
            await export_ldpred2(conn, study_id, output_path)

        content = output_path.read_text()
        lines = content.strip().split("\n")[1:]

        expected_neff = 4 / (1 / 40000 + 1 / 60000)

        for line in lines:
            parts = line.split("\t")
            n_eff = float(parts[6])
            assert abs(n_eff - expected_neff) < 1

    async def test_export_chromosome_format(self, db_with_gwas_data):
        from vcf_pg_loader.export.prs_formats import export_ldpred2

        pool, study_id = db_with_gwas_data

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            output_path = Path(f.name)

        async with pool.acquire() as conn:
            await export_ldpred2(conn, study_id, output_path)

        content = output_path.read_text()
        lines = content.strip().split("\n")[1:]

        for line in lines:
            parts = line.split("\t")
            chrom = parts[0]
            assert chrom.isdigit() or chrom in ["X", "Y", "M"]

    async def test_export_allele_orientation(self, db_with_gwas_data):
        from vcf_pg_loader.export.prs_formats import export_ldpred2

        pool, study_id = db_with_gwas_data

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            output_path = Path(f.name)

        async with pool.acquire() as conn:
            await export_ldpred2(conn, study_id, output_path)

        content = output_path.read_text()
        lines = content.strip().split("\n")[1:]

        for line in lines:
            parts = line.split("\t")
            a0, a1 = parts[2], parts[3]
            assert a0 != a1

    async def test_export_with_filter(self, db_with_gwas_data):
        from vcf_pg_loader.export.prs_formats import VariantFilter, export_ldpred2

        pool, study_id = db_with_gwas_data

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            output_path = Path(f.name)

        variant_filter = VariantFilter(hapmap3_only=True)

        async with pool.acquire() as conn:
            count = await export_ldpred2(conn, study_id, output_path, variant_filter)

        assert count == 5


class TestPRSice2Export:
    """Test PRSice-2 format export."""

    async def test_export_basic_format(self, db_with_gwas_data):
        from vcf_pg_loader.export.prs_formats import export_prsice2

        pool, study_id = db_with_gwas_data

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            output_path = Path(f.name)

        async with pool.acquire() as conn:
            count = await export_prsice2(conn, study_id, output_path)

        assert count == 6

        content = output_path.read_text()
        lines = content.strip().split("\n")

        assert lines[0] == "SNP\tA1\tA2\tBETA\tSE\tP"

    async def test_export_includes_all_stats(self, db_with_gwas_data):
        from vcf_pg_loader.export.prs_formats import export_prsice2

        pool, study_id = db_with_gwas_data

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            output_path = Path(f.name)

        async with pool.acquire() as conn:
            await export_prsice2(conn, study_id, output_path)

        content = output_path.read_text()
        lines = content.strip().split("\n")[1:]

        for line in lines:
            parts = line.split("\t")
            assert len(parts) == 6
            snp, a1, a2, beta, se, p = parts
            float(beta)
            float(se)
            float(p)

    async def test_export_with_filter(self, db_with_gwas_data):
        from vcf_pg_loader.export.prs_formats import VariantFilter, export_prsice2

        pool, study_id = db_with_gwas_data

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            output_path = Path(f.name)

        variant_filter = VariantFilter(min_maf=0.1)

        async with pool.acquire() as conn:
            count = await export_prsice2(conn, study_id, output_path, variant_filter)

        assert count == 5


class TestAlleleOrientation:
    """Test allele orientation handling across export formats."""

    async def test_effect_allele_alt_true(self, db_with_gwas_data):
        from vcf_pg_loader.export.prs_formats import export_plink_score

        pool, study_id = db_with_gwas_data

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            output_path = Path(f.name)

        async with pool.acquire() as conn:
            await export_plink_score(conn, study_id, output_path)

            variant = await conn.fetchrow(
                """
                SELECT v.rs_id, v.ref, v.alt, g.effect_allele, g.is_effect_allele_alt
                FROM gwas_summary_stats g
                JOIN variants v ON v.variant_id = g.variant_id AND v.chrom = 'chr1'
                WHERE g.study_id = $1 AND v.pos = 100
                """,
                study_id,
            )

        content = output_path.read_text()
        lines = content.strip().split("\n")[1:]

        rs100_line = [line for line in lines if line.startswith("rs100")][0]
        parts = rs100_line.split("\t")
        exported_a1 = parts[1]

        assert exported_a1 == variant["effect_allele"]

    async def test_effect_allele_alt_false(self, db_with_gwas_data):
        from vcf_pg_loader.export.prs_formats import export_plink_score

        pool, study_id = db_with_gwas_data

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            output_path = Path(f.name)

        async with pool.acquire() as conn:
            await export_plink_score(conn, study_id, output_path)

            variant = await conn.fetchrow(
                """
                SELECT v.rs_id, v.ref, v.alt, g.effect_allele, g.is_effect_allele_alt
                FROM gwas_summary_stats g
                JOIN variants v ON v.variant_id = g.variant_id AND v.chrom = 'chr1'
                WHERE g.study_id = $1 AND v.pos = 300
                """,
                study_id,
            )

        content = output_path.read_text()
        lines = content.strip().split("\n")[1:]

        rs300_line = [line for line in lines if line.startswith("rs300")][0]
        parts = rs300_line.split("\t")
        exported_a1 = parts[1]

        assert variant["is_effect_allele_alt"] is False
        assert exported_a1 == variant["effect_allele"]


class TestOutputFormat:
    """Test output file format specifications."""

    async def test_unix_line_endings(self, db_with_gwas_data):
        from vcf_pg_loader.export.prs_formats import export_plink_score

        pool, study_id = db_with_gwas_data

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            output_path = Path(f.name)

        async with pool.acquire() as conn:
            await export_plink_score(conn, study_id, output_path)

        content = output_path.read_bytes()
        assert b"\r\n" not in content
        assert b"\n" in content

    async def test_utf8_encoding(self, db_with_gwas_data):
        from vcf_pg_loader.export.prs_formats import export_plink_score

        pool, study_id = db_with_gwas_data

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            output_path = Path(f.name)

        async with pool.acquire() as conn:
            await export_plink_score(conn, study_id, output_path)

        content = output_path.read_bytes()
        content.decode("utf-8")

    async def test_tab_separated(self, db_with_gwas_data):
        from vcf_pg_loader.export.prs_formats import export_plink_score

        pool, study_id = db_with_gwas_data

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            output_path = Path(f.name)

        async with pool.acquire() as conn:
            await export_plink_score(conn, study_id, output_path)

        content = output_path.read_text()
        lines = content.strip().split("\n")

        for line in lines:
            assert "\t" in line
            assert "  " not in line


class TestVariantFilterDataclass:
    """Test VariantFilter dataclass functionality."""

    def test_default_values(self):
        from vcf_pg_loader.export.prs_formats import VariantFilter

        vf = VariantFilter()
        assert vf.hapmap3_only is False
        assert vf.min_info is None
        assert vf.min_maf is None

    def test_custom_values(self):
        from vcf_pg_loader.export.prs_formats import VariantFilter

        vf = VariantFilter(hapmap3_only=True, min_info=0.8, min_maf=0.01)
        assert vf.hapmap3_only is True
        assert vf.min_info == 0.8
        assert vf.min_maf == 0.01


class TestStudyNotFound:
    """Test error handling for missing study."""

    async def test_invalid_study_id(self, db_with_gwas_data):
        from vcf_pg_loader.export.prs_formats import export_plink_score

        pool, _ = db_with_gwas_data

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            output_path = Path(f.name)

        async with pool.acquire() as conn:
            count = await export_plink_score(conn, 99999, output_path)

        assert count == 0


class TestNeffComputation:
    """Test effective sample size computation for LDpred2."""

    async def test_case_control_neff(self, pg_pool):
        from vcf_pg_loader.export.prs_formats import export_ldpred2
        from vcf_pg_loader.gwas.schema import GWASSchemaManager
        from vcf_pg_loader.schema import SchemaManager

        async with pg_pool.acquire() as conn:
            schema_mgr = SchemaManager(human_genome=True)
            await schema_mgr.create_schema(conn, skip_encryption=True, skip_emergency=True)

            gwas_mgr = GWASSchemaManager()
            await gwas_mgr.create_gwas_schema(conn)

            batch_id = uuid.uuid4()
            await conn.execute(
                """
                INSERT INTO variants (chrom, pos, pos_range, ref, alt, rs_id, load_batch_id)
                VALUES ('chr1', 100::bigint, int8range(100::bigint, 101::bigint), 'A', 'G', 'rs1', $1)
                """,
                batch_id,
            )

            study_id = await conn.fetchval(
                """
                INSERT INTO studies (study_accession, trait_name, genome_build, n_cases, n_controls)
                VALUES ('GCST_CC', 'Case-Control', 'GRCh38', 5000, 15000)
                RETURNING study_id
                """
            )

            variant_id = await conn.fetchval(
                "SELECT variant_id FROM variants WHERE chrom = 'chr1' AND pos = 100"
            )

            await conn.execute(
                """
                INSERT INTO gwas_summary_stats (
                    variant_id, study_id, effect_allele, other_allele, beta, standard_error, p_value, is_effect_allele_alt
                ) VALUES ($1, $2, 'G', 'A', 0.05, 0.01, 1e-5, TRUE)
                """,
                variant_id,
                study_id,
            )

            with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
                output_path = Path(f.name)

            await export_ldpred2(conn, study_id, output_path)

        content = output_path.read_text()
        lines = content.strip().split("\n")
        data_line = lines[1]
        n_eff = float(data_line.split("\t")[6])

        expected_neff = 4 / (1 / 5000 + 1 / 15000)
        assert abs(n_eff - expected_neff) < 1

    async def test_quantitative_trait_neff(self, pg_pool):
        from vcf_pg_loader.export.prs_formats import export_ldpred2
        from vcf_pg_loader.gwas.schema import GWASSchemaManager
        from vcf_pg_loader.schema import SchemaManager

        async with pg_pool.acquire() as conn:
            schema_mgr = SchemaManager(human_genome=True)
            await schema_mgr.create_schema(conn, skip_encryption=True, skip_emergency=True)

            gwas_mgr = GWASSchemaManager()
            await gwas_mgr.create_gwas_schema(conn)

            batch_id = uuid.uuid4()
            await conn.execute(
                """
                INSERT INTO variants (chrom, pos, pos_range, ref, alt, rs_id, load_batch_id)
                VALUES ('chr1', 100::bigint, int8range(100::bigint, 101::bigint), 'A', 'G', 'rs1', $1)
                """,
                batch_id,
            )

            study_id = await conn.fetchval(
                """
                INSERT INTO studies (study_accession, trait_name, genome_build, sample_size)
                VALUES ('GCST_QT', 'Quantitative', 'GRCh38', 50000)
                RETURNING study_id
                """
            )

            variant_id = await conn.fetchval(
                "SELECT variant_id FROM variants WHERE chrom = 'chr1' AND pos = 100"
            )

            await conn.execute(
                """
                INSERT INTO gwas_summary_stats (
                    variant_id, study_id, effect_allele, other_allele, beta, standard_error, p_value, is_effect_allele_alt
                ) VALUES ($1, $2, 'G', 'A', 0.05, 0.01, 1e-5, TRUE)
                """,
                variant_id,
                study_id,
            )

            with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
                output_path = Path(f.name)

            await export_ldpred2(conn, study_id, output_path)

        content = output_path.read_text()
        lines = content.strip().split("\n")
        data_line = lines[1]
        n_eff = float(data_line.split("\t")[6])

        assert abs(n_eff - 50000) < 1


class TestErrorHandling:
    """Test error handling in export functions."""

    async def test_export_plink_score_propagates_database_errors(self, pg_pool):
        """Verify that database errors are propagated, not silently swallowed."""
        import asyncpg

        from vcf_pg_loader.export.prs_formats import export_plink_score

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            output_path = Path(f.name)

        async with pg_pool.acquire() as conn:
            await conn.execute("DROP TABLE IF EXISTS variants CASCADE")

            with pytest.raises(asyncpg.UndefinedTableError):
                await export_plink_score(conn, 999, output_path)
