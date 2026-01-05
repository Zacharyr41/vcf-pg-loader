"""GWAS summary statistics loader following GWAS-SSF standard."""

import csv
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import TypedDict

import asyncpg

from ..utils.variant_matching import build_variant_lookups
from ..utils.variant_matching import match_variant as shared_match_variant
from .models import GWASSummaryStatRecord, HarmonizationResult
from .schema import GWASSchemaManager

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {
    "chromosome",
    "base_pair_location",
    "effect_allele",
    "other_allele",
    "p_value",
}

COLUMN_ALIASES = {
    "chr": "chromosome",
    "chrom": "chromosome",
    "pos": "base_pair_location",
    "position": "base_pair_location",
    "bp": "base_pair_location",
    "a1": "effect_allele",
    "ea": "effect_allele",
    "a2": "other_allele",
    "oa": "other_allele",
    "nea": "other_allele",
    "pval": "p_value",
    "pvalue": "p_value",
    "p": "p_value",
    "se": "standard_error",
    "eaf": "effect_allele_frequency",
    "freq": "effect_allele_frequency",
    "maf": "effect_allele_frequency",
    "or": "odds_ratio",
    "n": "n",
    "n_total": "n",
    "rs": "rsid",
    "snp": "rsid",
    "marker": "rsid",
}


class GWASParseError(Exception):
    """Error parsing GWAS-SSF file."""

    pass


class GWASImportResult(TypedDict):
    """Result of GWAS import operation."""

    study_id: int
    stats_imported: int
    stats_matched: int
    stats_unmatched: int


def is_strand_ambiguous(allele1: str, allele2: str) -> bool:
    """Check if a SNP is strand-ambiguous (A/T or C/G)."""
    pair = frozenset([allele1.upper(), allele2.upper()])
    return pair in (frozenset(["A", "T"]), frozenset(["C", "G"]))


def complement_allele(allele: str) -> str:
    """Return the complement of a nucleotide allele."""
    complements = {"A": "T", "T": "A", "C": "G", "G": "C"}
    return complements.get(allele.upper(), allele)


def compute_is_effect_allele_alt(
    effect_allele: str,
    other_allele: str,
    ref: str,
    alt: str,
) -> bool | None:
    """Compute whether effect allele corresponds to VCF ALT.

    Returns:
        True if effect allele matches ALT
        False if effect allele matches REF
        None if alleles don't match (after considering strand flip)
    """
    ea = effect_allele.upper()
    oa = other_allele.upper()
    vcf_ref = ref.upper()
    vcf_alt = alt.upper()

    if ea == vcf_alt and oa == vcf_ref:
        return True
    if ea == vcf_ref and oa == vcf_alt:
        return False

    ea_comp = complement_allele(ea)
    oa_comp = complement_allele(oa)

    if ea_comp == vcf_alt and oa_comp == vcf_ref:
        return True
    if ea_comp == vcf_ref and oa_comp == vcf_alt:
        return False

    return None


def harmonize_alleles(
    effect_allele: str,
    other_allele: str,
    ref: str,
    alt: str,
    effect_allele_frequency: float | None = None,
    vcf_alt_frequency: float | None = None,
) -> HarmonizationResult:
    """Harmonize GWAS alleles to VCF REF/ALT orientation.

    For strand-ambiguous SNPs (A/T, C/G), uses allele frequency when available.
    """
    ea = effect_allele.upper()
    oa = other_allele.upper()
    vcf_ref = ref.upper()
    vcf_alt = alt.upper()

    if {ea, oa} == {vcf_ref, vcf_alt}:
        is_effect_alt = ea == vcf_alt
        return HarmonizationResult(
            is_match=True,
            is_flipped=False,
            is_effect_allele_alt=is_effect_alt,
            harmonized_effect_allele=ea,
            harmonized_other_allele=oa,
        )

    ea_comp = complement_allele(ea)
    oa_comp = complement_allele(oa)

    if {ea_comp, oa_comp} == {vcf_ref, vcf_alt}:
        is_effect_alt = ea_comp == vcf_alt
        return HarmonizationResult(
            is_match=True,
            is_flipped=True,
            is_effect_allele_alt=is_effect_alt,
            harmonized_effect_allele=ea_comp,
            harmonized_other_allele=oa_comp,
        )

    if is_strand_ambiguous(ea, oa):
        if effect_allele_frequency is not None and vcf_alt_frequency is not None:
            freq_diff_direct = abs(effect_allele_frequency - vcf_alt_frequency)
            freq_diff_flipped = abs((1 - effect_allele_frequency) - vcf_alt_frequency)

            if freq_diff_direct < 0.1:
                return HarmonizationResult(
                    is_match=True,
                    is_flipped=False,
                    is_effect_allele_alt=True,
                    harmonized_effect_allele=ea,
                    harmonized_other_allele=oa,
                )
            elif freq_diff_flipped < 0.1:
                return HarmonizationResult(
                    is_match=True,
                    is_flipped=False,
                    is_effect_allele_alt=False,
                    harmonized_effect_allele=ea,
                    harmonized_other_allele=oa,
                )

    return HarmonizationResult(is_match=False)


def match_variant(
    chromosome: str,
    position: int,
    effect_allele: str,
    other_allele: str,
    rsid: str | None,
    variant_lookup: dict[tuple[str, int, str, str], int],
    rsid_lookup: dict[str, int],
) -> int | None:
    """Match a GWAS variant to the variants table.

    Delegates to shared utility for consistent chromosome normalization.
    """
    return shared_match_variant(
        chromosome=chromosome,
        position=position,
        effect_allele=effect_allele,
        other_allele=other_allele,
        rsid=rsid,
        variant_lookup=variant_lookup,
        rsid_lookup=rsid_lookup,
    )


class GWASSSFParser:
    """Parser for GWAS-SSF format TSV files."""

    def __init__(self, path: Path):
        self.path = path
        self.columns: list[str] = []
        self.column_indices: dict[str, int] = {}
        self._parse_header()

    def _parse_header(self) -> None:
        """Parse and validate TSV header."""
        with open(self.path) as f:
            reader = csv.reader(f, delimiter="\t")
            header = next(reader)

        self.columns = []
        self.column_indices = {}

        for idx, col in enumerate(header):
            normalized = col.lower().strip()
            canonical = COLUMN_ALIASES.get(normalized, normalized)
            self.columns.append(canonical)
            self.column_indices[canonical] = idx

        missing = REQUIRED_COLUMNS - set(self.columns)
        if missing:
            raise GWASParseError(f"Missing required columns: {', '.join(sorted(missing))}")

    def has_required_columns(self) -> bool:
        """Check if all required columns are present."""
        return REQUIRED_COLUMNS.issubset(set(self.columns))

    def iter_records(self) -> Iterator[GWASSummaryStatRecord]:
        """Iterate over records in the file."""
        with open(self.path) as f:
            reader = csv.reader(f, delimiter="\t")
            next(reader)

            for line_num, row in enumerate(reader, start=2):
                try:
                    yield self._parse_row(row, line_num)
                except ValueError as e:
                    raise GWASParseError(f"Error parsing line {line_num}: {e}") from e

    def _parse_row(self, row: list[str], line_num: int) -> GWASSummaryStatRecord:
        """Parse a single row into a GWASSummaryStatRecord."""

        def get_value(col: str) -> str | None:
            if col not in self.column_indices:
                return None
            idx = self.column_indices[col]
            if idx >= len(row):
                return None
            val = row[idx].strip()
            return val if val else None

        def get_float(col: str) -> float | None:
            val = get_value(col)
            if val is None or val == "":
                return None
            try:
                return float(val)
            except ValueError as e:
                raise ValueError(f"Invalid float value '{val}' for column {col}") from e

        def get_int(col: str) -> int | None:
            val = get_value(col)
            if val is None or val == "":
                return None
            try:
                return int(float(val))
            except ValueError as e:
                raise ValueError(f"Invalid integer value '{val}' for column {col}") from e

        chromosome = get_value("chromosome")
        position_str = get_value("base_pair_location")
        effect_allele = get_value("effect_allele")
        other_allele = get_value("other_allele")
        p_value_str = get_value("p_value")

        if not chromosome:
            raise ValueError("chromosome is required")
        if not position_str:
            raise ValueError("base_pair_location is required")
        if not effect_allele:
            raise ValueError("effect_allele is required")
        if not p_value_str:
            raise ValueError("p_value is required")

        try:
            position = int(position_str)
        except ValueError as e:
            raise ValueError(f"Invalid position value: {position_str}") from e

        try:
            p_value = float(p_value_str)
        except ValueError as e:
            raise ValueError(f"Invalid p_value: {p_value_str}") from e

        return GWASSummaryStatRecord(
            chromosome=chromosome,
            position=position,
            effect_allele=effect_allele,
            other_allele=other_allele,
            p_value=p_value,
            rsid=get_value("rsid"),
            beta=get_float("beta"),
            odds_ratio=get_float("odds_ratio"),
            standard_error=get_float("standard_error"),
            effect_allele_frequency=get_float("effect_allele_frequency"),
            n_total=get_int("n"),
            n_cases=get_int("n_cases"),
            info_score=get_float("info"),
        )


class GWASLoader:
    """Load GWAS summary statistics into PostgreSQL."""

    def __init__(self, batch_size: int = 10000):
        self.batch_size = batch_size
        self.schema_manager = GWASSchemaManager()

    async def import_gwas(
        self,
        conn: asyncpg.Connection,
        tsv_path: Path,
        study_accession: str,
        trait_name: str | None = None,
        trait_ontology_id: str | None = None,
        publication_pmid: str | None = None,
        sample_size: int | None = None,
        n_cases: int | None = None,
        n_controls: int | None = None,
        genome_build: str = "GRCh38",
        analysis_software: str | None = None,
    ) -> GWASImportResult:
        """Import GWAS summary statistics from a TSV file.

        Args:
            conn: Database connection
            tsv_path: Path to GWAS-SSF format TSV file
            study_accession: GWAS Catalog accession (e.g., GCST90002357)
            trait_name: Human-readable trait name
            trait_ontology_id: EFO ontology ID
            publication_pmid: PubMed ID
            sample_size: Total sample size
            n_cases: Number of cases (for binary traits)
            n_controls: Number of controls (for binary traits)
            genome_build: Reference genome build
            analysis_software: Software used for analysis

        Returns:
            GWASImportResult with import statistics
        """
        existing = await self.schema_manager.get_study_by_accession(conn, study_accession)
        if existing:
            study_id = existing["study_id"]
            logger.info(f"Using existing study: {study_accession} (id={study_id})")
        else:
            study_id = await self.schema_manager.create_study(
                conn,
                study_accession=study_accession,
                trait_name=trait_name,
                trait_ontology_id=trait_ontology_id,
                publication_pmid=publication_pmid,
                sample_size=sample_size,
                n_cases=n_cases,
                n_controls=n_controls,
                genome_build=genome_build,
                analysis_software=analysis_software,
            )
            logger.info(f"Created study: {study_accession} (id={study_id})")

        variant_lookup, rsid_lookup = await self._build_variant_lookups(conn)

        parser = GWASSSFParser(tsv_path)

        stats_imported = 0
        stats_matched = 0
        stats_unmatched = 0
        batch = []

        for record in parser.iter_records():
            variant_id = match_variant(
                chromosome=record.chromosome,
                position=record.position,
                effect_allele=record.effect_allele,
                other_allele=record.other_allele or "",
                rsid=record.rsid,
                variant_lookup=variant_lookup,
                rsid_lookup=rsid_lookup,
            )

            is_effect_allele_alt = None
            if variant_id is not None:
                variant_info = await self._get_variant_info(conn, variant_id)
                if variant_info:
                    is_effect_allele_alt = compute_is_effect_allele_alt(
                        effect_allele=record.effect_allele,
                        other_allele=record.other_allele or "",
                        ref=variant_info["ref"],
                        alt=variant_info["alt"],
                    )
                stats_matched += 1
            else:
                stats_unmatched += 1

            batch.append(
                (
                    variant_id,
                    study_id,
                    record.effect_allele,
                    record.other_allele,
                    record.beta,
                    record.odds_ratio,
                    record.standard_error,
                    record.p_value,
                    record.effect_allele_frequency,
                    record.n_total,
                    record.n_cases,
                    record.info_score,
                    is_effect_allele_alt,
                )
            )

            if len(batch) >= self.batch_size:
                await self._insert_batch(conn, batch)
                stats_imported += len(batch)
                batch = []

        if batch:
            await self._insert_batch(conn, batch)
            stats_imported += len(batch)

        logger.info(
            f"Imported {stats_imported} statistics for study {study_accession} "
            f"(matched: {stats_matched}, unmatched: {stats_unmatched})"
        )

        return GWASImportResult(
            study_id=study_id,
            stats_imported=stats_imported,
            stats_matched=stats_matched,
            stats_unmatched=stats_unmatched,
        )

    async def _build_variant_lookups(
        self, conn: asyncpg.Connection
    ) -> tuple[dict[tuple[str, int, str, str], int], dict[str, int]]:
        """Build lookup dictionaries for variant matching.

        Delegates to shared utility for consistent chromosome normalization.
        """
        return await build_variant_lookups(conn)

    async def _get_variant_info(self, conn: asyncpg.Connection, variant_id: int) -> dict | None:
        """Get variant REF/ALT for harmonization."""
        row = await conn.fetchrow(
            "SELECT ref, alt FROM variants WHERE variant_id = $1",
            variant_id,
        )
        return dict(row) if row else None

    async def _insert_batch(self, conn: asyncpg.Connection, batch: list[tuple]) -> None:
        """Insert a batch of summary statistics."""
        await conn.executemany(
            """
            INSERT INTO gwas_summary_stats (
                variant_id, study_id, effect_allele, other_allele,
                beta, odds_ratio, standard_error, p_value,
                effect_allele_frequency, n_total, n_cases, info_score,
                is_effect_allele_alt
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (variant_id, study_id) DO UPDATE SET
                effect_allele = EXCLUDED.effect_allele,
                other_allele = EXCLUDED.other_allele,
                beta = EXCLUDED.beta,
                odds_ratio = EXCLUDED.odds_ratio,
                standard_error = EXCLUDED.standard_error,
                p_value = EXCLUDED.p_value,
                effect_allele_frequency = EXCLUDED.effect_allele_frequency,
                n_total = EXCLUDED.n_total,
                n_cases = EXCLUDED.n_cases,
                info_score = EXCLUDED.info_score,
                is_effect_allele_alt = EXCLUDED.is_effect_allele_alt
            """,
            batch,
        )
