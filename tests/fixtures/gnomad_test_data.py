"""Test fixtures for gnomAD-style population frequency data."""

GNOMAD_INFO_COMPLETE = {
    "AC": 1500,
    "AN": 100000,
    "AF": 0.015,
    "AC_AFR": 800,
    "AN_AFR": 20000,
    "AF_AFR": 0.04,
    "nhomalt_AFR": 32,
    "AC_AMR": 150,
    "AN_AMR": 15000,
    "AF_AMR": 0.01,
    "nhomalt_AMR": 1,
    "AC_ASJ": 50,
    "AN_ASJ": 5000,
    "AF_ASJ": 0.01,
    "nhomalt_ASJ": 0,
    "AC_EAS": 100,
    "AN_EAS": 10000,
    "AF_EAS": 0.01,
    "nhomalt_EAS": 0,
    "AC_FIN": 200,
    "AN_FIN": 10000,
    "AF_FIN": 0.02,
    "nhomalt_FIN": 1,
    "AC_NFE": 180,
    "AN_NFE": 30000,
    "AF_NFE": 0.006,
    "nhomalt_NFE": 0,
    "AC_SAS": 20,
    "AN_SAS": 10000,
    "AF_SAS": 0.002,
    "nhomalt_SAS": 0,
    "faf95_AFR": 0.035,
    "faf95_AMR": 0.008,
    "faf95_EAS": 0.008,
    "faf95_NFE": 0.005,
    "faf95_SAS": 0.001,
}

GNOMAD_INFO_PARTIAL = {
    "AC": 500,
    "AN": 50000,
    "AF": 0.01,
    "AC_AFR": 300,
    "AN_AFR": 10000,
    "AF_AFR": 0.03,
    "AC_NFE": 150,
    "AN_NFE": 30000,
    "AF_NFE": 0.005,
}

GNOMAD_INFO_RARE = {
    "AC": 5,
    "AN": 100000,
    "AF": 0.00005,
    "AC_AFR": 2,
    "AN_AFR": 20000,
    "AF_AFR": 0.0001,
    "nhomalt_AFR": 0,
    "AC_AMR": 1,
    "AN_AMR": 15000,
    "AF_AMR": 0.000067,
    "nhomalt_AMR": 0,
    "AC_ASJ": 0,
    "AN_ASJ": 5000,
    "AF_ASJ": 0.0,
    "nhomalt_ASJ": 0,
    "AC_EAS": 0,
    "AN_EAS": 10000,
    "AF_EAS": 0.0,
    "nhomalt_EAS": 0,
    "AC_FIN": 0,
    "AN_FIN": 10000,
    "AF_FIN": 0.0,
    "nhomalt_FIN": 0,
    "AC_NFE": 2,
    "AN_NFE": 30000,
    "AF_NFE": 0.000067,
    "nhomalt_NFE": 0,
    "AC_SAS": 0,
    "AN_SAS": 10000,
    "AF_SAS": 0.0,
    "nhomalt_SAS": 0,
}

GNOMAD_INFO_BOTTLENECKED_HIGH = {
    "AC": 100,
    "AN": 50000,
    "AF": 0.002,
    "AC_AFR": 10,
    "AN_AFR": 20000,
    "AF_AFR": 0.0005,
    "AC_ASJ": 80,
    "AN_ASJ": 5000,
    "AF_ASJ": 0.016,
    "AC_FIN": 5,
    "AN_FIN": 10000,
    "AF_FIN": 0.0005,
    "AC_NFE": 5,
    "AN_NFE": 15000,
    "AF_NFE": 0.00033,
}

GNOMAD_V4_INFO = {
    "AC": 2000,
    "AN": 150000,
    "AF": 0.0133,
    "gnomad_AC_afr": 900,
    "gnomad_AN_afr": 30000,
    "gnomad_AF_afr": 0.03,
    "gnomad_AC_amr": 200,
    "gnomad_AN_amr": 20000,
    "gnomad_AF_amr": 0.01,
    "gnomad_AC_asj": 50,
    "gnomad_AN_asj": 5000,
    "gnomad_AF_asj": 0.01,
    "gnomad_AC_eas": 150,
    "gnomad_AN_eas": 15000,
    "gnomad_AF_eas": 0.01,
    "gnomad_AC_fin": 100,
    "gnomad_AN_fin": 10000,
    "gnomad_AF_fin": 0.01,
    "gnomad_AC_nfe": 500,
    "gnomad_AN_nfe": 50000,
    "gnomad_AF_nfe": 0.01,
    "gnomad_AC_sas": 100,
    "gnomad_AN_sas": 20000,
    "gnomad_AF_sas": 0.005,
}

TOPMED_INFO = {
    "TOPMED_AC": 1000,
    "TOPMED_AN": 125000,
    "TOPMED_AF": 0.008,
    "TOPMED_AC_afr": 600,
    "TOPMED_AN_afr": 40000,
    "TOPMED_AF_afr": 0.015,
    "TOPMED_AC_amr": 150,
    "TOPMED_AN_amr": 25000,
    "TOPMED_AF_amr": 0.006,
    "TOPMED_AC_eas": 50,
    "TOPMED_AN_eas": 10000,
    "TOPMED_AF_eas": 0.005,
    "TOPMED_AC_eur": 200,
    "TOPMED_AN_eur": 50000,
    "TOPMED_AF_eur": 0.004,
}


def generate_bulk_gnomad_records(n_variants: int = 100000) -> list[dict]:
    """Generate bulk gnomAD-style INFO records for performance testing."""
    import random

    random.seed(42)
    records = []

    populations = ["AFR", "AMR", "ASJ", "EAS", "FIN", "NFE", "SAS"]
    pop_sizes = {
        "AFR": 20000,
        "AMR": 15000,
        "ASJ": 5000,
        "EAS": 10000,
        "FIN": 10000,
        "NFE": 30000,
        "SAS": 10000,
    }

    for _ in range(n_variants):
        total_ac = random.randint(1, 5000)
        total_an = sum(pop_sizes.values())

        info = {
            "AC": total_ac,
            "AN": total_an,
            "AF": total_ac / total_an,
        }

        remaining_ac = total_ac
        for pop in populations:
            if remaining_ac <= 0:
                pop_ac = 0
            else:
                pop_ac = min(remaining_ac, random.randint(0, remaining_ac))
                remaining_ac -= pop_ac

            pop_an = pop_sizes[pop]
            pop_af = pop_ac / pop_an if pop_an > 0 else 0

            info[f"AC_{pop}"] = pop_ac
            info[f"AN_{pop}"] = pop_an
            info[f"AF_{pop}"] = pop_af
            info[f"nhomalt_{pop}"] = random.randint(0, max(0, pop_ac // 10))

        records.append(
            {
                "chrom": f"chr{random.randint(1, 22)}",
                "pos": random.randint(1, 250000000),
                "ref": random.choice(["A", "C", "G", "T"]),
                "alt": random.choice(["A", "C", "G", "T"]),
                "info": info,
            }
        )

    return records
