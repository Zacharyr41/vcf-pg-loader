"""Tests for genotype data storage with hard calls and dosages.

TDD tests for:
- FORMAT field parsing (GT, GQ, DP, AD, DS, GP)
- Allele balance computation
- Dosage computation from GP when DS missing
- ADJ filter logic (GQ>=20, DP>=10, AB>=0.2 for hets)
- Hash partition distribution
- Dosage constraint enforcement

Reference: gnomAD ADJ filter criteria for high-quality genotypes.
"""

import asyncpg
import pytest


class TestParseGenotypeFields:
    """Test parse_genotype_fields for all supported FORMAT fields."""

    def test_parse_gt_unphased(self):
        from vcf_pg_loader.genotypes.genotype_loader import parse_genotype_fields

        result = parse_genotype_fields("0/1", None, None, None, None, None)
        assert result.gt == "0/1"
        assert result.phased is False

    def test_parse_gt_phased(self):
        from vcf_pg_loader.genotypes.genotype_loader import parse_genotype_fields

        result = parse_genotype_fields("0|1", None, None, None, None, None)
        assert result.gt == "0|1"
        assert result.phased is True

    def test_parse_gq(self):
        from vcf_pg_loader.genotypes.genotype_loader import parse_genotype_fields

        result = parse_genotype_fields("0/1", 30, None, None, None, None)
        assert result.gq == 30

    def test_parse_dp(self):
        from vcf_pg_loader.genotypes.genotype_loader import parse_genotype_fields

        result = parse_genotype_fields("0/1", None, 25, None, None, None)
        assert result.dp == 25

    def test_parse_ad(self):
        from vcf_pg_loader.genotypes.genotype_loader import parse_genotype_fields

        result = parse_genotype_fields("0/1", None, None, [10, 15], None, None)
        assert result.ad == [10, 15]

    def test_parse_dosage(self):
        from vcf_pg_loader.genotypes.genotype_loader import parse_genotype_fields

        result = parse_genotype_fields("0/1", None, None, None, 1.0, None)
        assert result.dosage == 1.0

    def test_parse_gp(self):
        from vcf_pg_loader.genotypes.genotype_loader import parse_genotype_fields

        result = parse_genotype_fields("0/1", None, None, None, None, [0.1, 0.8, 0.1])
        assert result.gp == [0.1, 0.8, 0.1]

    def test_parse_all_fields(self):
        from vcf_pg_loader.genotypes.genotype_loader import parse_genotype_fields

        result = parse_genotype_fields(
            gt="0|1",
            gq=35,
            dp=40,
            ad=[20, 20],
            ds=1.0,
            gp=[0.05, 0.90, 0.05],
        )
        assert result.gt == "0|1"
        assert result.phased is True
        assert result.gq == 35
        assert result.dp == 40
        assert result.ad == [20, 20]
        assert result.dosage == 1.0
        assert result.gp == [0.05, 0.90, 0.05]

    def test_parse_missing_genotype(self):
        from vcf_pg_loader.genotypes.genotype_loader import parse_genotype_fields

        result = parse_genotype_fields("./.", None, None, None, None, None)
        assert result.gt == "./."
        assert result.phased is False


class TestComputeAlleleBalance:
    """Test allele balance computation from AD field."""

    def test_balanced_het(self):
        from vcf_pg_loader.genotypes.genotype_loader import compute_allele_balance

        ab = compute_allele_balance([20, 20])
        assert ab == pytest.approx(0.5)

    def test_ref_biased(self):
        from vcf_pg_loader.genotypes.genotype_loader import compute_allele_balance

        ab = compute_allele_balance([30, 10])
        assert ab == pytest.approx(0.25)

    def test_alt_biased(self):
        from vcf_pg_loader.genotypes.genotype_loader import compute_allele_balance

        ab = compute_allele_balance([10, 30])
        assert ab == pytest.approx(0.75)

    def test_homozygous_ref(self):
        from vcf_pg_loader.genotypes.genotype_loader import compute_allele_balance

        ab = compute_allele_balance([40, 0])
        assert ab == pytest.approx(0.0)

    def test_homozygous_alt(self):
        from vcf_pg_loader.genotypes.genotype_loader import compute_allele_balance

        ab = compute_allele_balance([0, 40])
        assert ab == pytest.approx(1.0)

    def test_empty_ad_returns_none(self):
        from vcf_pg_loader.genotypes.genotype_loader import compute_allele_balance

        ab = compute_allele_balance([])
        assert ab is None

    def test_none_ad_returns_none(self):
        from vcf_pg_loader.genotypes.genotype_loader import compute_allele_balance

        ab = compute_allele_balance(None)
        assert ab is None

    def test_zero_total_depth_returns_none(self):
        from vcf_pg_loader.genotypes.genotype_loader import compute_allele_balance

        ab = compute_allele_balance([0, 0])
        assert ab is None

    def test_multiallelic_sums_alt_depths(self):
        from vcf_pg_loader.genotypes.genotype_loader import compute_allele_balance

        ab = compute_allele_balance([10, 20, 10])
        assert ab == pytest.approx(0.75)


class TestDosageFromGP:
    """Test dosage computation from genotype probabilities."""

    def test_homozygous_ref(self):
        from vcf_pg_loader.genotypes.genotype_loader import dosage_from_gp

        dosage = dosage_from_gp([1.0, 0.0, 0.0])
        assert dosage == pytest.approx(0.0)

    def test_heterozygous(self):
        from vcf_pg_loader.genotypes.genotype_loader import dosage_from_gp

        dosage = dosage_from_gp([0.0, 1.0, 0.0])
        assert dosage == pytest.approx(1.0)

    def test_homozygous_alt(self):
        from vcf_pg_loader.genotypes.genotype_loader import dosage_from_gp

        dosage = dosage_from_gp([0.0, 0.0, 1.0])
        assert dosage == pytest.approx(2.0)

    def test_uncertain_het(self):
        from vcf_pg_loader.genotypes.genotype_loader import dosage_from_gp

        dosage = dosage_from_gp([0.1, 0.8, 0.1])
        expected = 0.8 + 2 * 0.1
        assert dosage == pytest.approx(expected)

    def test_none_gp_returns_none(self):
        from vcf_pg_loader.genotypes.genotype_loader import dosage_from_gp

        dosage = dosage_from_gp(None)
        assert dosage is None

    def test_empty_gp_returns_none(self):
        from vcf_pg_loader.genotypes.genotype_loader import dosage_from_gp

        dosage = dosage_from_gp([])
        assert dosage is None

    def test_invalid_gp_length_returns_none(self):
        from vcf_pg_loader.genotypes.genotype_loader import dosage_from_gp

        dosage = dosage_from_gp([0.5, 0.5])
        assert dosage is None


class TestADJFilterLogic:
    """Test ADJ filter criteria: GQ>=20, DP>=10, AB>=0.2 for hets."""

    def test_passes_all_criteria(self):
        from vcf_pg_loader.genotypes.genotype_loader import evaluate_adj_filter

        passes = evaluate_adj_filter(
            gt="0/1",
            gq=25,
            dp=15,
            allele_balance=0.4,
        )
        assert passes is True

    def test_fails_gq_below_threshold(self):
        from vcf_pg_loader.genotypes.genotype_loader import evaluate_adj_filter

        passes = evaluate_adj_filter(
            gt="0/1",
            gq=19,
            dp=15,
            allele_balance=0.4,
        )
        assert passes is False

    def test_passes_gq_at_boundary(self):
        from vcf_pg_loader.genotypes.genotype_loader import evaluate_adj_filter

        passes = evaluate_adj_filter(
            gt="0/1",
            gq=20,
            dp=15,
            allele_balance=0.4,
        )
        assert passes is True

    def test_fails_dp_below_threshold(self):
        from vcf_pg_loader.genotypes.genotype_loader import evaluate_adj_filter

        passes = evaluate_adj_filter(
            gt="0/1",
            gq=25,
            dp=9,
            allele_balance=0.4,
        )
        assert passes is False

    def test_passes_dp_at_boundary(self):
        from vcf_pg_loader.genotypes.genotype_loader import evaluate_adj_filter

        passes = evaluate_adj_filter(
            gt="0/1",
            gq=25,
            dp=10,
            allele_balance=0.4,
        )
        assert passes is True

    def test_fails_ab_below_threshold_for_het(self):
        from vcf_pg_loader.genotypes.genotype_loader import evaluate_adj_filter

        passes = evaluate_adj_filter(
            gt="0/1",
            gq=25,
            dp=15,
            allele_balance=0.19,
        )
        assert passes is False

    def test_passes_ab_at_boundary_for_het(self):
        from vcf_pg_loader.genotypes.genotype_loader import evaluate_adj_filter

        passes = evaluate_adj_filter(
            gt="0/1",
            gq=25,
            dp=15,
            allele_balance=0.2,
        )
        assert passes is True

    def test_ab_not_checked_for_hom_ref(self):
        from vcf_pg_loader.genotypes.genotype_loader import evaluate_adj_filter

        passes = evaluate_adj_filter(
            gt="0/0",
            gq=25,
            dp=15,
            allele_balance=0.05,
        )
        assert passes is True

    def test_ab_not_checked_for_hom_alt(self):
        from vcf_pg_loader.genotypes.genotype_loader import evaluate_adj_filter

        passes = evaluate_adj_filter(
            gt="1/1",
            gq=25,
            dp=15,
            allele_balance=0.95,
        )
        assert passes is True

    def test_phased_het_checks_ab(self):
        from vcf_pg_loader.genotypes.genotype_loader import evaluate_adj_filter

        passes = evaluate_adj_filter(
            gt="0|1",
            gq=25,
            dp=15,
            allele_balance=0.15,
        )
        assert passes is False

    def test_phased_het_reverse_checks_ab(self):
        from vcf_pg_loader.genotypes.genotype_loader import evaluate_adj_filter

        passes = evaluate_adj_filter(
            gt="1|0",
            gq=25,
            dp=15,
            allele_balance=0.15,
        )
        assert passes is False

    def test_missing_gq_passes(self):
        from vcf_pg_loader.genotypes.genotype_loader import evaluate_adj_filter

        passes = evaluate_adj_filter(
            gt="0/1",
            gq=None,
            dp=15,
            allele_balance=0.4,
        )
        assert passes is True

    def test_missing_dp_passes(self):
        from vcf_pg_loader.genotypes.genotype_loader import evaluate_adj_filter

        passes = evaluate_adj_filter(
            gt="0/1",
            gq=25,
            dp=None,
            allele_balance=0.4,
        )
        assert passes is True

    def test_missing_ab_passes_for_het(self):
        from vcf_pg_loader.genotypes.genotype_loader import evaluate_adj_filter

        passes = evaluate_adj_filter(
            gt="0/1",
            gq=25,
            dp=15,
            allele_balance=None,
        )
        assert passes is True


class TestDosageConstraint:
    """Test dosage value constraint enforcement (0 <= dosage <= 2)."""

    def test_valid_dosage_zero(self):
        from vcf_pg_loader.genotypes.genotype_loader import validate_dosage

        assert validate_dosage(0.0) is True

    def test_valid_dosage_one(self):
        from vcf_pg_loader.genotypes.genotype_loader import validate_dosage

        assert validate_dosage(1.0) is True

    def test_valid_dosage_two(self):
        from vcf_pg_loader.genotypes.genotype_loader import validate_dosage

        assert validate_dosage(2.0) is True

    def test_valid_dosage_fractional(self):
        from vcf_pg_loader.genotypes.genotype_loader import validate_dosage

        assert validate_dosage(1.5) is True

    def test_invalid_dosage_negative(self):
        from vcf_pg_loader.genotypes.genotype_loader import validate_dosage

        assert validate_dosage(-0.1) is False

    def test_invalid_dosage_above_two(self):
        from vcf_pg_loader.genotypes.genotype_loader import validate_dosage

        assert validate_dosage(2.1) is False

    def test_none_dosage_valid(self):
        from vcf_pg_loader.genotypes.genotype_loader import validate_dosage

        assert validate_dosage(None) is True


class TestPartitionDistribution:
    """Test hash partitioning distributes samples across 16 partitions."""

    def test_partition_calculation(self):
        from vcf_pg_loader.genotypes.genotype_loader import get_partition_number

        sample_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 100, 1000]
        partitions = [get_partition_number(sid, 16) for sid in sample_ids]

        for p in partitions:
            assert 0 <= p < 16

    def test_same_sample_same_partition(self):
        from vcf_pg_loader.genotypes.genotype_loader import get_partition_number

        p1 = get_partition_number(42, 16)
        p2 = get_partition_number(42, 16)
        assert p1 == p2

    def test_distribution_across_partitions(self):
        from vcf_pg_loader.genotypes.genotype_loader import get_partition_number

        partition_counts = [0] * 16
        for sample_id in range(1, 10001):
            p = get_partition_number(sample_id, 16)
            partition_counts[p] += 1

        for count in partition_counts:
            assert count > 0


class TestGenotypeRecord:
    """Test GenotypeRecord dataclass."""

    def test_record_creation(self):
        from vcf_pg_loader.genotypes.genotype_loader import GenotypeRecord

        record = GenotypeRecord(
            variant_id=12345,
            sample_id=1,
            gt="0/1",
            phased=False,
            gq=30,
            dp=25,
            ad=[10, 15],
            dosage=1.0,
            gp=[0.1, 0.8, 0.1],
            allele_balance=0.6,
        )
        assert record.variant_id == 12345
        assert record.sample_id == 1
        assert record.gt == "0/1"
        assert record.phased is False
        assert record.gq == 30
        assert record.dp == 25
        assert record.ad == [10, 15]
        assert record.dosage == 1.0
        assert record.gp == [0.1, 0.8, 0.1]
        assert record.allele_balance == 0.6

    def test_record_to_db_row(self):
        from vcf_pg_loader.genotypes.genotype_loader import GenotypeRecord

        record = GenotypeRecord(
            variant_id=12345,
            sample_id=1,
            gt="0/1",
            phased=False,
            gq=30,
            dp=25,
            ad=[10, 15],
            dosage=1.0,
            gp=[0.1, 0.8, 0.1],
            allele_balance=0.6,
        )
        row = record.to_db_row()
        assert row["variant_id"] == 12345
        assert row["sample_id"] == 1
        assert row["gt"] == "0/1"
        assert row["phased"] is False
        assert row["gq"] == 30
        assert row["dp"] == 25
        assert row["ad"] == [10, 15]
        assert row["dosage"] == 1.0
        assert row["gp"] == [0.1, 0.8, 0.1]
        assert row["allele_balance"] == 0.6


class TestGenotypeSchemaCreation:
    """Test genotypes table schema creation."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_create_genotypes_table(self, db_pool):
        from vcf_pg_loader.genotypes.schema import GenotypesSchemaManager

        async with db_pool.acquire() as conn:
            schema_manager = GenotypesSchemaManager()
            await schema_manager.create_genotypes_schema(conn)

            table_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'genotypes'
                )
            """)
            assert table_exists is True

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_partitions_created(self, db_pool):
        from vcf_pg_loader.genotypes.schema import GenotypesSchemaManager

        async with db_pool.acquire() as conn:
            schema_manager = GenotypesSchemaManager()
            await schema_manager.create_genotypes_schema(conn)

            partition_count = await conn.fetchval("""
                SELECT COUNT(*)
                FROM pg_inherits
                WHERE inhparent = 'genotypes'::regclass
            """)
            assert partition_count == 16

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_passes_adj_generated_column(self, db_pool):
        from vcf_pg_loader.genotypes.schema import GenotypesSchemaManager

        async with db_pool.acquire() as conn:
            schema_manager = GenotypesSchemaManager()
            await schema_manager.create_genotypes_schema(conn)

            column_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns
                    WHERE table_name = 'genotypes'
                    AND column_name = 'passes_adj'
                )
            """)
            assert column_exists is True

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_dosage_constraint(self, db_pool):
        from vcf_pg_loader.genotypes.schema import GenotypesSchemaManager

        async with db_pool.acquire() as conn:
            schema_manager = GenotypesSchemaManager()
            await schema_manager.create_genotypes_schema(conn)

            await conn.execute("""
                INSERT INTO samples (external_id) VALUES ('test_sample')
            """)
            sample_id = await conn.fetchval(
                "SELECT sample_id FROM samples WHERE external_id = 'test_sample'"
            )

            with pytest.raises(asyncpg.exceptions.CheckViolationError):
                await conn.execute(
                    """
                    INSERT INTO genotypes (variant_id, sample_id, gt, dosage)
                    VALUES (1, $1, '0/1', 2.5)
                    """,
                    sample_id,
                )

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_indexes_created(self, db_pool):
        from vcf_pg_loader.genotypes.schema import GenotypesSchemaManager

        async with db_pool.acquire() as conn:
            schema_manager = GenotypesSchemaManager()
            await schema_manager.create_genotypes_schema(conn)

            indexes = await conn.fetch("""
                SELECT indexname FROM pg_indexes
                WHERE tablename = 'genotypes'
            """)
            index_names = [r["indexname"] for r in indexes]

            assert any("adj" in name.lower() for name in index_names)
            assert any("dosage" in name.lower() for name in index_names)


@pytest.fixture
def postgres_container():
    """Provide a PostgreSQL test container."""
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:15") as postgres:
        yield postgres


@pytest.fixture
async def db_pool(postgres_container):
    """Provide an async database connection pool."""
    import asyncpg

    from vcf_pg_loader.schema import SchemaManager

    host = postgres_container.get_container_host_ip()
    port = postgres_container.get_exposed_port(5432)
    user = postgres_container.username
    password = postgres_container.password
    database = postgres_container.dbname

    pool = await asyncpg.create_pool(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        min_size=2,
        max_size=4,
    )

    async with pool.acquire() as conn:
        schema_manager = SchemaManager()
        await schema_manager.create_schema(conn)

    yield pool
    await pool.close()


@pytest.mark.integration
class TestGenotypeLoaderIntegration:
    """Integration tests for loading multi-sample VCF with FORMAT fields."""

    @pytest.mark.asyncio
    async def test_load_genotypes_from_vcf(self, db_pool, tmp_path):
        from vcf_pg_loader.genotypes.genotype_loader import GenotypeLoader
        from vcf_pg_loader.genotypes.schema import GenotypesSchemaManager

        vcf_content = """##fileformat=VCFv4.3
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype Quality">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read Depth">
##FORMAT=<ID=AD,Number=R,Type=Integer,Description="Allelic Depths">
##FORMAT=<ID=DS,Number=1,Type=Float,Description="Dosage">
##FORMAT=<ID=GP,Number=G,Type=Float,Description="Genotype Probabilities">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE1\tSAMPLE2
chr1\t100\t.\tA\tG\t30\tPASS\t.\tGT:GQ:DP:AD:DS:GP\t0/1:35:40:20,20:1.0:0.05,0.90,0.05\t0/0:45:50:50,0:0.0:0.99,0.01,0.00
chr1\t200\t.\tC\tT\t30\tPASS\t.\tGT:GQ:DP:AD:DS:GP\t1/1:30:25:0,25:2.0:0.00,0.02,0.98\t0/1:25:30:12,18:1.0:0.10,0.80,0.10
"""
        vcf_path = tmp_path / "test.vcf"
        vcf_path.write_text(vcf_content)

        async with db_pool.acquire() as conn:
            schema_manager = GenotypesSchemaManager()
            await schema_manager.create_genotypes_schema(conn)

            for external_id in ["SAMPLE1", "SAMPLE2"]:
                await conn.execute(
                    "INSERT INTO samples (external_id) VALUES ($1) ON CONFLICT DO NOTHING",
                    external_id,
                )

            loader = GenotypeLoader()
            await loader.load_from_vcf(conn, vcf_path, variant_id_start=1)

            count = await conn.fetchval("SELECT COUNT(*) FROM genotypes")
            assert count == 4

            sample1_het = await conn.fetchrow("""
                SELECT gt, gq, dp, ad, dosage, passes_adj
                FROM genotypes g
                JOIN samples s ON g.sample_id = s.sample_id
                WHERE s.external_id = 'SAMPLE1' AND g.variant_id = 1
            """)
            assert sample1_het["gt"] == "0/1"
            assert sample1_het["gq"] == 35
            assert sample1_het["dp"] == 40
            assert sample1_het["dosage"] == pytest.approx(1.0)
            assert sample1_het["passes_adj"] is True

    @pytest.mark.asyncio
    async def test_adj_filter_only_mode(self, db_pool, tmp_path):
        from vcf_pg_loader.genotypes.genotype_loader import GenotypeLoader
        from vcf_pg_loader.genotypes.schema import GenotypesSchemaManager

        vcf_content = """##fileformat=VCFv4.3
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype Quality">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read Depth">
##FORMAT=<ID=AD,Number=R,Type=Integer,Description="Allelic Depths">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE1\tSAMPLE2
chr1\t100\t.\tA\tG\t30\tPASS\t.\tGT:GQ:DP:AD\t0/1:35:40:20,20\t0/1:15:5:4,1
"""
        vcf_path = tmp_path / "test_adj.vcf"
        vcf_path.write_text(vcf_content)

        async with db_pool.acquire() as conn:
            schema_manager = GenotypesSchemaManager()
            await schema_manager.create_genotypes_schema(conn)

            for external_id in ["SAMPLE1", "SAMPLE2"]:
                await conn.execute(
                    "INSERT INTO samples (external_id) VALUES ($1) ON CONFLICT DO NOTHING",
                    external_id,
                )

            loader = GenotypeLoader(adj_filter=True)
            await loader.load_from_vcf(conn, vcf_path, variant_id_start=1)

            count = await conn.fetchval("SELECT COUNT(*) FROM genotypes")
            assert count == 1

    @pytest.mark.asyncio
    async def test_dosage_only_mode(self, db_pool, tmp_path):
        from vcf_pg_loader.genotypes.genotype_loader import GenotypeLoader
        from vcf_pg_loader.genotypes.schema import GenotypesSchemaManager

        vcf_content = """##fileformat=VCFv4.3
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DS,Number=1,Type=Float,Description="Dosage">
##contig=<ID=chr1,length=248956422>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE1
chr1\t100\t.\tA\tG\t30\tPASS\t.\tGT:DS\t0/1:1.0
"""
        vcf_path = tmp_path / "test_dosage.vcf"
        vcf_path.write_text(vcf_content)

        async with db_pool.acquire() as conn:
            schema_manager = GenotypesSchemaManager()
            await schema_manager.create_genotypes_schema(conn)

            await conn.execute(
                "INSERT INTO samples (external_id) VALUES ($1) ON CONFLICT DO NOTHING",
                "SAMPLE1",
            )

            loader = GenotypeLoader(dosage_only=True)
            await loader.load_from_vcf(conn, vcf_path, variant_id_start=1)

            row = await conn.fetchrow("SELECT gt, dosage FROM genotypes")
            assert row["gt"] == "."
            assert row["dosage"] == pytest.approx(1.0)
