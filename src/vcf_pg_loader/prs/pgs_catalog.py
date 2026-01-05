"""PGS Catalog format parser and allele harmonization."""

from collections.abc import Iterator
from pathlib import Path

from .models import HarmonizationResult, PGSMetadata, PRSWeight


class PGSParseError(Exception):
    """Error parsing PGS Catalog file."""

    pass


class GenomeBuildMismatchError(Exception):
    """PGS file genome build doesn't match database variants."""

    pass


BUILD_ALIASES = {
    "hg38": "GRCh38",
    "hg19": "GRCh37",
    "grch38": "GRCh38",
    "grch37": "GRCh37",
}

COLUMN_ALIASES = {
    "rsid": "rsid",
    "rs_id": "rsid",
    "snp": "rsid",
    "chr_name": "chr_name",
    "chrom": "chr_name",
    "chromosome": "chr_name",
    "chr_position": "chr_position",
    "pos": "chr_position",
    "position": "chr_position",
    "effect_allele": "effect_allele",
    "a1": "effect_allele",
    "ea": "effect_allele",
    "other_allele": "other_allele",
    "a2": "other_allele",
    "oa": "other_allele",
    "nea": "other_allele",
    "effect_weight": "effect_weight",
    "weight": "effect_weight",
    "beta": "effect_weight",
    "allelefrequency_effect": "allele_frequency",
    "eaf": "allele_frequency",
    "freq": "allele_frequency",
    "is_interaction": "is_interaction",
    "is_haplotype": "is_haplotype",
    "is_dominant": "is_dominant",
    "is_recessive": "is_recessive",
    "locus_name": "locus_name",
    "gene": "locus_name",
}


def parse_pgs_header(header_lines: list[str]) -> PGSMetadata:
    """Parse PGS Catalog header metadata from comment lines.

    Args:
        header_lines: List of header lines starting with # or ###

    Returns:
        PGSMetadata with extracted values
    """
    metadata: dict = {}

    for line in header_lines:
        if line.startswith("###"):
            continue
        if line.startswith("#"):
            line = line[1:].strip()
            if "=" in line:
                key, value = line.split("=", 1)
                key = key.strip().lower()
                value = value.strip()

                if key == "pgs_id":
                    metadata["pgs_id"] = value
                elif key == "trait_name":
                    metadata["trait_name"] = value
                elif key == "trait_ontology_id":
                    metadata["trait_ontology_id"] = value
                elif key == "publication_pmid":
                    metadata["publication_pmid"] = value
                elif key == "genome_build":
                    metadata["genome_build"] = value
                elif key == "weight_type":
                    metadata["weight_type"] = value
                elif key == "n_variants":
                    try:
                        metadata["n_variants"] = int(value)
                    except ValueError:
                        pass
                elif key == "reporting_ancestry":
                    metadata["reporting_ancestry"] = value

    if "pgs_id" not in metadata:
        raise PGSParseError("Missing required header field: pgs_id")
    if "genome_build" not in metadata:
        raise PGSParseError("Missing required header field: genome_build")

    return PGSMetadata(
        pgs_id=metadata["pgs_id"],
        genome_build=metadata["genome_build"],
        trait_name=metadata.get("trait_name"),
        trait_ontology_id=metadata.get("trait_ontology_id"),
        publication_pmid=metadata.get("publication_pmid"),
        weight_type=metadata.get("weight_type"),
        n_variants=metadata.get("n_variants"),
        reporting_ancestry=metadata.get("reporting_ancestry"),
    )


def normalize_build(build: str) -> str:
    """Normalize genome build string to GRCh format."""
    return BUILD_ALIASES.get(build.lower(), build)


def validate_genome_build(pgs_build: str, db_build: str) -> bool:
    """Validate PGS file genome build matches database.

    Args:
        pgs_build: Genome build from PGS file header
        db_build: Genome build of variants in database

    Returns:
        True if builds match

    Raises:
        GenomeBuildMismatchError: If builds don't match
    """
    pgs_normalized = normalize_build(pgs_build)
    db_normalized = normalize_build(db_build)

    if pgs_normalized.lower() != db_normalized.lower():
        raise GenomeBuildMismatchError(
            f"PGS file genome build ({pgs_build}) does not match " f"database variants ({db_build})"
        )

    return True


def is_strand_ambiguous(allele1: str, allele2: str) -> bool:
    """Check if a SNP is strand-ambiguous (A/T or C/G)."""
    pair = frozenset([allele1.upper(), allele2.upper()])
    return pair in (frozenset(["A", "T"]), frozenset(["C", "G"]))


def complement_allele(allele: str) -> str:
    """Return the complement of a nucleotide allele."""
    complements = {"A": "T", "T": "A", "C": "G", "G": "C"}
    return "".join(complements.get(b.upper(), b) for b in allele)


def harmonize_weight_allele(weight: PRSWeight, ref: str, alt: str) -> HarmonizationResult:
    """Harmonize PRS weight allele to VCF REF/ALT orientation.

    Args:
        weight: PRSWeight with effect_allele and other_allele
        ref: VCF REF allele
        alt: VCF ALT allele

    Returns:
        HarmonizationResult with match status and orientation
    """
    ea = weight.effect_allele.upper()
    oa = (weight.other_allele or "").upper()
    vcf_ref = ref.upper()
    vcf_alt = alt.upper()

    if {ea, oa} == {vcf_ref, vcf_alt} or (oa == "" and ea in {vcf_ref, vcf_alt}):
        is_effect_alt = ea == vcf_alt
        return HarmonizationResult(
            is_match=True,
            is_flipped=False,
            is_effect_allele_alt=is_effect_alt,
            harmonized_effect_allele=ea,
            harmonized_other_allele=oa if oa else (vcf_ref if is_effect_alt else vcf_alt),
        )

    ea_comp = complement_allele(ea)
    oa_comp = complement_allele(oa) if oa else ""

    if {ea_comp, oa_comp} == {vcf_ref, vcf_alt} or (
        oa_comp == "" and ea_comp in {vcf_ref, vcf_alt}
    ):
        is_effect_alt = ea_comp == vcf_alt
        return HarmonizationResult(
            is_match=True,
            is_flipped=True,
            is_effect_allele_alt=is_effect_alt,
            harmonized_effect_allele=ea_comp,
            harmonized_other_allele=oa_comp if oa_comp else (vcf_ref if is_effect_alt else vcf_alt),
        )

    return HarmonizationResult(is_match=False)


class PGSCatalogParser:
    """Parser for PGS Catalog scoring files."""

    def __init__(self, path: Path):
        self.path = path
        self._header_lines: list[str] = []
        self._data_start_line: int = 0
        self._columns: list[str] = []
        self._column_indices: dict[str, int] = {}
        self._metadata: PGSMetadata | None = None
        self._parse_header()

    @classmethod
    def from_string(cls, content: str) -> "PGSCatalogParser":
        """Create parser from string content (for testing)."""
        parser = object.__new__(cls)
        parser.path = None
        parser._header_lines = []
        parser._columns = []
        parser._column_indices = {}
        parser._metadata = None
        parser._content = content
        parser._parse_header_from_content(content)
        return parser

    def _parse_header(self) -> None:
        """Parse header from file."""
        with open(self.path) as f:
            content = f.read()
        self._content = content
        self._parse_header_from_content(content)

    def _parse_header_from_content(self, content: str) -> None:
        """Parse header from content string."""
        lines = content.split("\n")

        for i, line in enumerate(lines):
            if line.startswith("#"):
                self._header_lines.append(line)
            else:
                self._data_start_line = i
                if line.strip():
                    self._parse_column_header(line)
                break

        self._metadata = parse_pgs_header(self._header_lines)

    def _parse_column_header(self, line: str) -> None:
        """Parse TSV column header."""
        raw_columns = line.strip().split("\t")

        self._columns = []
        self._column_indices = {}

        for idx, col in enumerate(raw_columns):
            normalized = col.lower().strip()
            canonical = COLUMN_ALIASES.get(normalized, normalized)
            self._columns.append(canonical)
            self._column_indices[canonical] = idx

    @property
    def metadata(self) -> PGSMetadata:
        """Get parsed metadata."""
        if self._metadata is None:
            raise PGSParseError("Header not parsed")
        return self._metadata

    def iter_weights(self) -> Iterator[PRSWeight]:
        """Iterate over weight records in the file."""
        lines = self._content.split("\n")
        data_lines = lines[self._data_start_line + 1 :]

        for line_num, line in enumerate(data_lines, start=self._data_start_line + 2):
            line = line.strip()
            if not line:
                continue

            try:
                yield self._parse_row(line.split("\t"), line_num)
            except ValueError as e:
                raise PGSParseError(f"Error parsing line {line_num}: {e}") from e

    def _parse_row(self, row: list[str], line_num: int) -> PRSWeight:
        """Parse a single row into a PRSWeight."""

        def get_value(col: str) -> str | None:
            if col not in self._column_indices:
                return None
            idx = self._column_indices[col]
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
                return int(val)
            except ValueError as e:
                raise ValueError(f"Invalid integer value '{val}' for column {col}") from e

        def get_bool(col: str) -> bool:
            val = get_value(col)
            if val is None or val == "":
                return False
            return val.lower() in ("true", "1", "yes", "t")

        effect_allele = get_value("effect_allele")
        effect_weight = get_float("effect_weight")

        if not effect_allele:
            raise PGSParseError(f"Missing required column: effect_allele at line {line_num}")
        if effect_weight is None:
            raise PGSParseError(f"Missing required column: effect_weight at line {line_num}")

        return PRSWeight(
            effect_allele=effect_allele,
            effect_weight=effect_weight,
            chromosome=get_value("chr_name"),
            position=get_int("chr_position"),
            rsid=get_value("rsid"),
            other_allele=get_value("other_allele"),
            is_interaction=get_bool("is_interaction"),
            is_haplotype=get_bool("is_haplotype"),
            is_dominant=get_bool("is_dominant"),
            is_recessive=get_bool("is_recessive"),
            allele_frequency=get_float("allele_frequency"),
            locus_name=get_value("locus_name"),
        )
