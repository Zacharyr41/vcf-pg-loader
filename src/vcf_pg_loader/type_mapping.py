"""VCF to PostgreSQL type mapping."""


VCF_TO_PG_TYPE: dict[tuple[str, str], str] = {
    ('Integer', '0'): 'BOOLEAN',
    ('Integer', '1'): 'INTEGER',
    ('Integer', 'A'): 'INTEGER[]',
    ('Integer', 'R'): 'INTEGER[]',
    ('Integer', 'G'): 'INTEGER[]',
    ('Integer', '.'): 'INTEGER[]',
    ('Float', '0'): 'BOOLEAN',
    ('Float', '1'): 'REAL',
    ('Float', 'A'): 'REAL[]',
    ('Float', 'R'): 'REAL[]',
    ('Float', 'G'): 'REAL[]',
    ('Float', '.'): 'REAL[]',
    ('String', '0'): 'BOOLEAN',
    ('String', '1'): 'TEXT',
    ('String', 'A'): 'TEXT[]',
    ('String', 'R'): 'TEXT[]',
    ('String', 'G'): 'TEXT[]',
    ('String', '.'): 'TEXT[]',
    ('Character', '1'): 'CHAR(1)',
    ('Character', '.'): 'TEXT',
    ('Flag', '0'): 'BOOLEAN',
}


def get_pg_type(vcf_type: str, number: str) -> str:
    """
    Get PostgreSQL type for a VCF INFO/FORMAT field.

    Args:
        vcf_type: VCF type (Integer, Float, String, Character, Flag)
        number: VCF Number specification (0, 1, A, R, G, .)

    Returns:
        PostgreSQL column type as string
    """
    key = (vcf_type, number)
    if key in VCF_TO_PG_TYPE:
        return VCF_TO_PG_TYPE[key]

    if number.isdigit():
        n = int(number)
        if n == 0:
            return 'BOOLEAN'
        elif n == 1:
            if vcf_type == 'Integer':
                return 'INTEGER'
            elif vcf_type == 'Float':
                return 'REAL'
            else:
                return 'TEXT'
        else:
            if vcf_type == 'Integer':
                return 'INTEGER[]'
            elif vcf_type == 'Float':
                return 'REAL[]'
            else:
                return 'TEXT[]'

    if vcf_type == 'Flag':
        return 'BOOLEAN'
    elif vcf_type == 'Integer':
        return 'INTEGER[]'
    elif vcf_type == 'Float':
        return 'REAL[]'
    else:
        return 'TEXT'


def infer_column_definition(field_id: str, vcf_type: str, number: str) -> str:
    """
    Generate PostgreSQL column definition for a VCF field.

    Args:
        field_id: Field identifier (e.g., 'DP', 'AC', 'CSQ')
        vcf_type: VCF type
        number: VCF Number specification

    Returns:
        Column definition string (e.g., 'dp INTEGER', 'ac INTEGER[]')
    """
    pg_type = get_pg_type(vcf_type, number)
    col_name = field_id.lower()
    return f"{col_name} {pg_type}"


CLINVAR_SIGNIFICANCE_MAP: dict[str, str] = {
    'Benign': 'benign',
    'Likely_benign': 'likely_benign',
    'Uncertain_significance': 'vus',
    'Likely_pathogenic': 'likely_pathogenic',
    'Pathogenic': 'pathogenic',
    'Conflicting_interpretations_of_pathogenicity': 'conflicting',
    'drug_response': 'drug_response',
    'risk_factor': 'risk_factor',
    'association': 'association',
    'protective': 'protective',
    'not_provided': 'not_provided',
    'other': 'other',
}


def normalize_clinvar_significance(raw_sig: str) -> str:
    """
    Normalize ClinVar clinical significance to standardized value.

    Args:
        raw_sig: Raw ClinVar CLNSIG value

    Returns:
        Normalized significance string
    """
    if not raw_sig:
        return 'not_provided'

    parts = raw_sig.replace('/', ',').split(',')
    normalized_parts = []

    for part in parts:
        part = part.strip()
        normalized = CLINVAR_SIGNIFICANCE_MAP.get(part, part.lower())
        normalized_parts.append(normalized)

    if 'pathogenic' in normalized_parts:
        return 'pathogenic'
    if 'likely_pathogenic' in normalized_parts:
        return 'likely_pathogenic'
    if 'benign' in normalized_parts and 'pathogenic' not in str(normalized_parts):
        return 'benign'
    if 'likely_benign' in normalized_parts:
        return 'likely_benign'
    if 'conflicting' in normalized_parts:
        return 'conflicting'

    return normalized_parts[0] if normalized_parts else 'not_provided'


IMPACT_SEVERITY_ORDER = ['HIGH', 'MODERATE', 'LOW', 'MODIFIER']


def get_worst_impact(impacts: list) -> str:
    """
    Get the worst (most severe) impact from a list of impacts.

    Args:
        impacts: List of impact strings

    Returns:
        Most severe impact
    """
    if not impacts:
        return 'MODIFIER'

    for severity in IMPACT_SEVERITY_ORDER:
        if severity in impacts:
            return severity

    return impacts[0] if impacts else 'MODIFIER'
