"""Population frequency parsing and loading for gnomAD and similar resources.

This module provides functionality for:
- Parsing gnomAD INFO fields for population-specific frequencies
- Computing popmax while excluding bottlenecked populations
- Bulk loading population frequencies into the database

vcfanno Workflow Example:

    To pre-annotate VCF files with gnomAD frequencies using vcfanno:

    1. Create a vcfanno TOML config (gnomad.toml):
        [[annotation]]
        file = "gnomad.genomes.v3.1.sites.vcf.bgz"
        fields = ["AC", "AN", "AF", "AC_AFR", "AN_AFR", "AF_AFR", "nhomalt_AFR",
                  "AC_AMR", "AN_AMR", "AF_AMR", "nhomalt_AMR",
                  "AC_ASJ", "AN_ASJ", "AF_ASJ", "nhomalt_ASJ",
                  "AC_EAS", "AN_EAS", "AF_EAS", "nhomalt_EAS",
                  "AC_FIN", "AN_FIN", "AF_FIN", "nhomalt_FIN",
                  "AC_NFE", "AN_NFE", "AF_NFE", "nhomalt_NFE",
                  "AC_SAS", "AN_SAS", "AF_SAS", "nhomalt_SAS",
                  "faf95_AFR", "faf95_AMR", "faf95_EAS", "faf95_NFE", "faf95_SAS"]
        ops = ["self", "self", "self", "self", "self", "self", "self",
               "self", "self", "self", "self",
               "self", "self", "self", "self",
               "self", "self", "self", "self",
               "self", "self", "self", "self",
               "self", "self", "self", "self",
               "self", "self", "self", "self",
               "self", "self", "self", "self", "self"]
        names = ["gnomad_AC", "gnomad_AN", "gnomad_AF", ...]

    2. Run vcfanno:
        vcfanno -p 4 gnomad.toml input.vcf.gz | bgzip > annotated.vcf.gz

    3. Import frequencies:
        vcf-pg-loader import-frequencies gnomAD_v3 annotated.vcf.gz --db postgresql://...
"""

import logging
from dataclasses import dataclass
from typing import Any

import asyncpg

logger = logging.getLogger(__name__)

GNOMAD_POPULATIONS = ["AFR", "AMR", "ASJ", "EAS", "FIN", "NFE", "SAS"]

BOTTLENECKED_POPULATIONS = {"ASJ", "FIN"}


@dataclass
class PopulationFrequency:
    """Population-specific allele frequency data."""

    ac: int | None = None
    an: int | None = None
    af: float | None = None
    hom_count: int | None = None
    faf_95: float | None = None


def parse_gnomad_info(
    info: dict[str, Any],
    prefix: str = "",
) -> dict[str, PopulationFrequency]:
    """Parse gnomAD-style INFO fields into population frequencies.

    Supports gnomAD v2, v3, v4 formats and TOPMed.

    Args:
        info: VCF INFO field dictionary
        prefix: Optional prefix for field names (e.g., "gnomad_" for vcfanno output)

    Returns:
        Dictionary mapping population code to PopulationFrequency
    """
    result: dict[str, PopulationFrequency] = {}

    for pop in GNOMAD_POPULATIONS:
        pop_lower = pop.lower()

        ac_keys = [
            f"{prefix}AC_{pop}",
            f"{prefix}AC_{pop_lower}",
            f"AC_{pop}",
            f"AC_{pop_lower}",
        ]
        an_keys = [
            f"{prefix}AN_{pop}",
            f"{prefix}AN_{pop_lower}",
            f"AN_{pop}",
            f"AN_{pop_lower}",
        ]
        af_keys = [
            f"{prefix}AF_{pop}",
            f"{prefix}AF_{pop_lower}",
            f"AF_{pop}",
            f"AF_{pop_lower}",
        ]
        hom_keys = [
            f"{prefix}nhomalt_{pop}",
            f"{prefix}nhomalt_{pop_lower}",
            f"nhomalt_{pop}",
            f"nhomalt_{pop_lower}",
        ]
        faf_keys = [
            f"{prefix}faf95_{pop}",
            f"{prefix}faf95_{pop_lower}",
            f"faf95_{pop}",
            f"faf95_{pop_lower}",
        ]

        ac = _get_first_value(info, ac_keys)
        an = _get_first_value(info, an_keys)
        af = _get_first_value(info, af_keys)

        if ac is None and an is None and af is None:
            continue

        hom_count = _get_first_value(info, hom_keys)
        faf_95 = _get_first_value(info, faf_keys)

        result[pop] = PopulationFrequency(
            ac=_to_int(ac),
            an=_to_int(an),
            af=_to_float(af),
            hom_count=_to_int(hom_count),
            faf_95=_to_float(faf_95),
        )

    return result


def _get_first_value(info: dict, keys: list[str]) -> Any:
    """Get first matching value from info dict."""
    for key in keys:
        if key in info:
            return info[key]
    return None


def _to_int(value: Any) -> int | None:
    """Convert value to int or None."""
    if value is None:
        return None
    try:
        if isinstance(value, list):
            value = value[0] if value else None
        if value is None:
            return None
        return int(value)
    except (ValueError, TypeError):
        return None


def _to_float(value: Any) -> float | None:
    """Convert value to float or None."""
    if value is None:
        return None
    try:
        if isinstance(value, list):
            value = value[0] if value else None
        if value is None:
            return None
        return float(value)
    except (ValueError, TypeError):
        return None


def compute_popmax(
    frequencies: dict[str, PopulationFrequency],
    exclude_bottlenecked: bool = True,
) -> tuple[float | None, str | None]:
    """Compute population maximum allele frequency.

    gnomAD excludes bottlenecked populations (ASJ, FIN) from popmax by default
    because these populations have experienced genetic bottlenecks that can
    inflate allele frequencies for some variants.

    Args:
        frequencies: Dictionary mapping population to PopulationFrequency
        exclude_bottlenecked: If True, exclude ASJ and FIN from popmax calculation

    Returns:
        Tuple of (popmax_af, popmax_population) or (None, None) if no data
    """
    if not frequencies:
        return None, None

    max_af: float | None = None
    max_pop: str | None = None

    for pop, freq in frequencies.items():
        if exclude_bottlenecked and pop in BOTTLENECKED_POPULATIONS:
            continue

        if freq.af is not None:
            if max_af is None or freq.af > max_af:
                max_af = freq.af
                max_pop = pop
            elif freq.af == max_af and max_pop is None:
                max_pop = pop

    if max_af is None and frequencies:
        non_bottlenecked = [
            (pop, freq)
            for pop, freq in frequencies.items()
            if not exclude_bottlenecked or pop not in BOTTLENECKED_POPULATIONS
        ]
        if non_bottlenecked:
            pop, freq = non_bottlenecked[0]
            return freq.af, pop

    return max_af, max_pop


class PopulationFreqLoader:
    """Load population frequencies into PostgreSQL."""

    def __init__(self, batch_size: int = 10000):
        self.batch_size = batch_size

    async def import_variant_frequencies(
        self,
        conn: asyncpg.Connection,
        variant_id: int,
        info: dict[str, Any],
        source: str,
        subset: str = "all",
        prefix: str = "",
        update_popmax: bool = False,
    ) -> dict:
        """Import population frequencies for a single variant.

        Args:
            conn: Database connection
            variant_id: Variant ID in the variants table
            info: VCF INFO field dictionary
            source: Frequency source (gnomAD_v2.1, gnomAD_v3, gnomAD_v4, TOPMed)
            subset: Data subset (all, controls, non_neuro, non_cancer)
            prefix: Field name prefix for vcfanno output
            update_popmax: If True, update popmax columns in variants table

        Returns:
            Dictionary with import statistics
        """
        frequencies = parse_gnomad_info(info, prefix)

        if not frequencies:
            return {"frequencies_inserted": 0}

        batch = []
        for pop, freq in frequencies.items():
            batch.append(
                (
                    variant_id,
                    source,
                    pop,
                    subset,
                    freq.ac,
                    freq.an,
                    freq.af,
                    freq.hom_count,
                    freq.faf_95,
                )
            )

        await conn.executemany(
            """
            INSERT INTO population_frequencies
                (variant_id, source, population, subset, ac, an, af, hom_count, faf_95)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (variant_id, source, population, subset) DO UPDATE SET
                ac = EXCLUDED.ac,
                an = EXCLUDED.an,
                af = EXCLUDED.af,
                hom_count = EXCLUDED.hom_count,
                faf_95 = EXCLUDED.faf_95
            """,
            batch,
        )

        if update_popmax:
            popmax_af, popmax_pop = compute_popmax(frequencies)
            await conn.execute(
                """
                UPDATE variants
                SET gnomad_popmax_af = $2, gnomad_popmax_pop = $3
                WHERE variant_id = $1
                """,
                variant_id,
                popmax_af,
                popmax_pop,
            )

        return {"frequencies_inserted": len(batch)}

    async def import_batch_frequencies(
        self,
        conn: asyncpg.Connection,
        batch: list[tuple[int, dict[str, Any]]],
        source: str,
        subset: str = "all",
        prefix: str = "",
        update_popmax: bool = False,
    ) -> dict:
        """Import population frequencies for a batch of variants.

        Args:
            conn: Database connection
            batch: List of (variant_id, info_dict) tuples
            source: Frequency source
            subset: Data subset
            prefix: Field name prefix
            update_popmax: If True, update popmax columns

        Returns:
            Dictionary with import statistics
        """
        freq_records = []
        popmax_updates = []

        for variant_id, info in batch:
            frequencies = parse_gnomad_info(info, prefix)

            for pop, freq in frequencies.items():
                freq_records.append(
                    (
                        variant_id,
                        source,
                        pop,
                        subset,
                        freq.ac,
                        freq.an,
                        freq.af,
                        freq.hom_count,
                        freq.faf_95,
                    )
                )

            if update_popmax and frequencies:
                popmax_af, popmax_pop = compute_popmax(frequencies)
                popmax_updates.append((variant_id, popmax_af, popmax_pop))

        if freq_records:
            await conn.executemany(
                """
                INSERT INTO population_frequencies
                    (variant_id, source, population, subset, ac, an, af, hom_count, faf_95)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (variant_id, source, population, subset) DO UPDATE SET
                    ac = EXCLUDED.ac,
                    an = EXCLUDED.an,
                    af = EXCLUDED.af,
                    hom_count = EXCLUDED.hom_count,
                    faf_95 = EXCLUDED.faf_95
                """,
                freq_records,
            )

        if popmax_updates:
            await conn.executemany(
                """
                UPDATE variants
                SET gnomad_popmax_af = $2, gnomad_popmax_pop = $3
                WHERE variant_id = $1
                """,
                popmax_updates,
            )

        return {
            "frequencies_inserted": len(freq_records),
            "variants_processed": len(batch),
            "popmax_updated": len(popmax_updates),
        }
