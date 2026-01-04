"""SQL validation functions for in-database QC computations.

Provides SQL functions for:
- HWE exact test (Wigginton et al. 2005)
- Allele frequency from dosages
- Effective sample size for case-control
- Allele harmonization with strand flip support

Reference for HWE:
Wigginton JE, Cutler DJ, Abecasis GR. A note on exact tests of
Hardy-Weinberg equilibrium. Am J Hum Genet. 2005 May;76(5):887-93.
DOI: 10.1086/429864
"""

import asyncpg

from vcf_pg_loader.qc.variant_qc import compute_hwe_pvalue


async def create_validation_functions(conn: asyncpg.Connection) -> None:
    """Create all SQL validation functions in the database."""
    await _create_hwe_exact_test(conn)
    await _create_af_from_dosages(conn)
    await _create_n_eff(conn)
    await _create_alleles_match(conn)


async def _create_hwe_exact_test(conn: asyncpg.Connection) -> None:
    """Create HWE exact test function (Wigginton et al. 2005)."""
    await conn.execute("""
        CREATE OR REPLACE FUNCTION hwe_exact_test(n_aa INT, n_ab INT, n_bb INT)
        RETURNS FLOAT AS $$
        DECLARE
            n INT;
            n_a INT;
            n_b INT;
            min_het INT;
            max_het INT;
            mid INT;
            het_probs FLOAT[];
            total FLOAT;
            curr_het INT;
            prev_het INT;
            next_het INT;
            p_obs FLOAT;
            p_value FLOAT;
            p FLOAT;
            i INT;
            tmp_n_aa INT;
            tmp_n_bb INT;
        BEGIN
            n := n_aa + n_ab + n_bb;

            IF n = 0 THEN
                RETURN NULL;
            END IF;

            n_a := 2 * n_aa + n_ab;
            n_b := 2 * n_bb + n_ab;

            IF n_a = 0 OR n_b = 0 THEN
                RETURN 1.0;
            END IF;

            IF n_ab > LEAST(n_a, n_b) THEN
                RETURN NULL;
            END IF;

            min_het := ABS(n_a - n_b) % 2;
            max_het := LEAST(n_a, n_b);

            IF max_het < min_het THEN
                RETURN 1.0;
            END IF;

            het_probs := ARRAY_FILL(0.0::FLOAT, ARRAY[max_het + 1]);

            mid := (min_het + max_het) / 2;
            IF mid % 2 != min_het % 2 THEN
                IF mid < max_het THEN
                    mid := mid + 1;
                ELSE
                    mid := mid - 1;
                END IF;
            END IF;

            IF mid > max_het OR mid < min_het THEN
                mid := min_het;
            END IF;

            het_probs[mid + 1] := 1.0;
            total := 1.0;

            curr_het := mid;
            WHILE curr_het > min_het LOOP
                prev_het := curr_het - 2;
                IF prev_het < 0 THEN
                    EXIT;
                END IF;

                tmp_n_aa := (n_a - curr_het) / 2;
                tmp_n_bb := (n_b - curr_het) / 2;

                IF tmp_n_aa <= 0 OR tmp_n_bb <= 0 THEN
                    EXIT;
                END IF;

                het_probs[prev_het + 1] := het_probs[curr_het + 1] *
                    curr_het * (curr_het - 1) / (4.0 * tmp_n_aa * tmp_n_bb);
                total := total + het_probs[prev_het + 1];
                curr_het := prev_het;
            END LOOP;

            curr_het := mid;
            WHILE curr_het < max_het LOOP
                next_het := curr_het + 2;
                IF next_het > max_het THEN
                    EXIT;
                END IF;

                tmp_n_aa := (n_a - curr_het) / 2;
                tmp_n_bb := (n_b - curr_het) / 2;

                IF tmp_n_aa < 0 OR tmp_n_bb < 0 THEN
                    EXIT;
                END IF;

                het_probs[next_het + 1] := het_probs[curr_het + 1] *
                    4.0 * tmp_n_aa * tmp_n_bb / (next_het * (next_het - 1));
                total := total + het_probs[next_het + 1];
                curr_het := next_het;
            END LOOP;

            IF total > 0 THEN
                FOR i IN 1..ARRAY_LENGTH(het_probs, 1) LOOP
                    het_probs[i] := het_probs[i] / total;
                END LOOP;
            END IF;

            IF n_ab + 1 > ARRAY_LENGTH(het_probs, 1) THEN
                RETURN 1.0;
            END IF;

            p_obs := het_probs[n_ab + 1];

            p_value := 0.0;
            FOR i IN 1..ARRAY_LENGTH(het_probs, 1) LOOP
                IF het_probs[i] <= p_obs + 1e-10 THEN
                    p_value := p_value + het_probs[i];
                END IF;
            END LOOP;

            RETURN LEAST(1.0, p_value);
        END;
        $$ LANGUAGE plpgsql IMMUTABLE STRICT;
    """)


async def _create_af_from_dosages(conn: asyncpg.Connection) -> None:
    """Create allele frequency from dosages function."""
    await conn.execute("""
        CREATE OR REPLACE FUNCTION af_from_dosages(dosages FLOAT[])
        RETURNS FLOAT AS $$
            SELECT AVG(d) / 2.0 FROM unnest(dosages) AS d WHERE d IS NOT NULL;
        $$ LANGUAGE SQL IMMUTABLE STRICT;
    """)


async def _create_n_eff(conn: asyncpg.Connection) -> None:
    """Create effective sample size function for case-control studies."""
    await conn.execute("""
        CREATE OR REPLACE FUNCTION n_eff(n_cases INT, n_controls INT)
        RETURNS FLOAT AS $$
            SELECT CASE
                WHEN n_cases = 0 OR n_controls = 0 THEN NULL
                ELSE 4.0 * n_cases * n_controls / NULLIF(n_cases + n_controls, 0)
            END;
        $$ LANGUAGE SQL IMMUTABLE STRICT;
    """)


async def _create_alleles_match(conn: asyncpg.Connection) -> None:
    """Create allele harmonization check function with strand flip support."""
    await conn.execute("""
        CREATE OR REPLACE FUNCTION alleles_match(
            ref1 TEXT, alt1 TEXT, ref2 TEXT, alt2 TEXT
        ) RETURNS BOOLEAN AS $$
        DECLARE
            r1 TEXT;
            a1 TEXT;
            r2 TEXT;
            a2 TEXT;
            r1_comp TEXT;
            a1_comp TEXT;
        BEGIN
            IF ref1 IS NULL OR alt1 IS NULL OR ref2 IS NULL OR alt2 IS NULL THEN
                RETURN NULL;
            END IF;

            r1 := UPPER(ref1);
            a1 := UPPER(alt1);
            r2 := UPPER(ref2);
            a2 := UPPER(alt2);

            IF (r1 = r2 AND a1 = a2) THEN
                RETURN TRUE;
            END IF;

            IF (r1 = a2 AND a1 = r2) THEN
                RETURN TRUE;
            END IF;

            r1_comp := TRANSLATE(r1, 'ACGT', 'TGCA');
            a1_comp := TRANSLATE(a1, 'ACGT', 'TGCA');

            IF (r1_comp = r2 AND a1_comp = a2) THEN
                RETURN TRUE;
            END IF;

            IF (r1_comp = a2 AND a1_comp = r2) THEN
                RETURN TRUE;
            END IF;

            RETURN FALSE;
        END;
        $$ LANGUAGE plpgsql IMMUTABLE;
    """)


def hwe_exact_test_python(n_aa: int, n_ab: int, n_bb: int) -> float:
    """Python reference implementation of HWE exact test.

    Delegates to the existing implementation in qc.variant_qc module.

    Args:
        n_aa: Number of homozygous reference samples
        n_ab: Number of heterozygous samples
        n_bb: Number of homozygous alt samples

    Returns:
        Two-sided p-value for HWE exact test
    """
    return compute_hwe_pvalue(n_ab, n_aa, n_bb)


def af_from_dosages_python(dosages: list[float | None]) -> float | None:
    """Python reference implementation of allele frequency from dosages.

    Args:
        dosages: List of dosage values (0-2 scale), may contain None

    Returns:
        Allele frequency, or None if no valid dosages
    """
    valid = [d for d in dosages if d is not None]
    if not valid:
        return None
    return sum(valid) / (2.0 * len(valid))


def n_eff_python(n_cases: int, n_controls: int) -> float | None:
    """Python reference implementation of effective sample size.

    Args:
        n_cases: Number of cases
        n_controls: Number of controls

    Returns:
        Effective sample size, or None if either count is zero
    """
    if n_cases == 0 or n_controls == 0:
        return None
    return 4.0 * n_cases * n_controls / (n_cases + n_controls)


def alleles_match_python(ref1: str, alt1: str, ref2: str, alt2: str) -> bool | None:
    """Python reference implementation of allele matching with strand flip.

    Args:
        ref1: Reference allele from first source
        alt1: Alternate allele from first source
        ref2: Reference allele from second source
        alt2: Alternate allele from second source

    Returns:
        True if alleles match (directly or with strand flip), False otherwise,
        None if any input is None
    """
    if any(x is None for x in [ref1, alt1, ref2, alt2]):
        return None

    r1, a1 = ref1.upper(), alt1.upper()
    r2, a2 = ref2.upper(), alt2.upper()

    if (r1 == r2 and a1 == a2) or (r1 == a2 and a1 == r2):
        return True

    complement = str.maketrans("ACGT", "TGCA")
    r1_comp = r1.translate(complement)
    a1_comp = a1.translate(complement)

    if (r1_comp == r2 and a1_comp == a2) or (r1_comp == a2 and a1_comp == r2):
        return True

    return False
