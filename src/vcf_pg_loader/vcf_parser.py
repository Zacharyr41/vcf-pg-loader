"""VCF parsing functionality."""

import re
from collections.abc import Iterator
from math import comb
from pathlib import Path

from cyvcf2 import VCF

from .models import VariantRecord
from .normalizer import normalize_variant


def get_array_size(number_spec: str, n_alts: int, ploidy: int = 2) -> int:
    """Calculate expected array size for INFO/FORMAT fields."""
    if number_spec == 'A':
        return n_alts
    if number_spec == 'R':
        return n_alts + 1
    if number_spec == 'G':
        return comb(n_alts + ploidy, ploidy)
    if number_spec == '.':
        return -1  # Variable length
    try:
        return int(number_spec)
    except ValueError:
        return 1


class VCFHeaderParser:
    """Parser for VCF header information."""

    ANN_FIELDS = [
        'Allele', 'Annotation', 'Annotation_Impact', 'Gene_Name', 'Gene_ID',
        'Feature_Type', 'Feature_ID', 'Transcript_BioType', 'Rank', 'HGVS.c',
        'HGVS.p', 'cDNA.pos/cDNA.length', 'CDS.pos/CDS.length',
        'AA.pos/AA.length', 'Distance', 'ERRORS/WARNINGS/INFO'
    ]

    def __init__(self):
        self._info_fields: dict[str, dict[str, str]] = {}
        self._format_fields: dict[str, dict[str, str]] = {}
        self._samples: list[str] = []
        self._contigs: dict[str, dict[str, str]] = {}
        self._csq_fields: list[str] = []
        self._ann_fields: list[str] = []

    @property
    def samples(self) -> list[str]:
        """Return sample names from VCF header."""
        return self._samples

    @property
    def contigs(self) -> dict[str, dict[str, str]]:
        """Return contig information from VCF header."""
        return self._contigs

    @property
    def csq_fields(self) -> list[str]:
        """Return CSQ field names if present."""
        return self._csq_fields

    @property
    def ann_fields(self) -> list[str]:
        """Return ANN field names if present."""
        return self._ann_fields

    def parse_from_vcf(self, vcf) -> None:
        """Parse all header information from a cyvcf2 VCF object."""
        self.parse_info_fields_from_vcf(vcf)
        self.parse_format_fields_from_vcf(vcf)
        self._samples = list(vcf.samples)
        self._parse_contigs_from_vcf(vcf)
        self._parse_csq_from_vcf(vcf)
        self._parse_ann_from_vcf(vcf)

    def parse_info_fields_from_vcf(self, vcf) -> dict[str, dict[str, str]]:
        """Parse INFO field definitions from a cyvcf2 VCF object."""
        self._info_fields = {}
        for header in vcf.header_iter():
            info = header.info()
            if info.get('HeaderType') == 'INFO':
                field_id = info.get('ID')
                if field_id:
                    self._info_fields[field_id] = {
                        'Number': info.get('Number', '.'),
                        'Type': info.get('Type', 'String'),
                        'Description': info.get('Description', '').strip('"'),
                    }
        return self._info_fields

    def parse_format_fields_from_vcf(self, vcf) -> dict[str, dict[str, str]]:
        """Parse FORMAT field definitions from a cyvcf2 VCF object."""
        self._format_fields = {}
        for header in vcf.header_iter():
            info = header.info()
            if info.get('HeaderType') == 'FORMAT':
                field_id = info.get('ID')
                if field_id:
                    self._format_fields[field_id] = {
                        'Number': info.get('Number', '.'),
                        'Type': info.get('Type', 'String'),
                        'Description': info.get('Description', '').strip('"'),
                    }
        return self._format_fields

    def _parse_contigs_from_vcf(self, vcf) -> None:
        """Parse contig definitions from a cyvcf2 VCF object."""
        self._contigs = {}
        for header in vcf.header_iter():
            info = header.info()
            if info.get('HeaderType') == 'CONTIG':
                contig_id = info.get('ID')
                if contig_id:
                    self._contigs[contig_id] = {
                        k: v for k, v in info.items()
                        if k not in ('HeaderType', 'ID')
                    }

    def _parse_csq_from_vcf(self, vcf) -> None:
        """Parse VEP CSQ field structure from a cyvcf2 VCF object."""
        self._csq_fields = []
        try:
            csq_info = vcf.get_header_type('CSQ')
            if csq_info:
                desc = csq_info.get('Description', '')
                if 'Format:' in desc:
                    format_part = desc.split('Format:')[-1].strip().strip('"')
                    self._csq_fields = format_part.split('|')
        except KeyError:
            pass

    def _parse_ann_from_vcf(self, vcf) -> None:
        """Parse SnpEff ANN field structure from a cyvcf2 VCF object."""
        self._ann_fields = []
        try:
            ann_info = vcf.get_header_type('ANN')
            if ann_info:
                desc = ann_info.get('Description', '')
                if "'" in desc and '|' in desc:
                    start = desc.find("'")
                    end = desc.rfind("'")
                    if start < end:
                        format_part = desc[start+1:end]
                        self._ann_fields = [f.strip() for f in format_part.split('|')]
                if not self._ann_fields:
                    self._ann_fields = self.ANN_FIELDS.copy()
        except KeyError:
            pass

    def get_info_field(self, field_id: str) -> dict[str, str] | None:
        """Get metadata for a specific INFO field."""
        return self._info_fields.get(field_id)

    def get_format_field(self, field_id: str) -> dict[str, str] | None:
        """Get metadata for a specific FORMAT field."""
        return self._format_fields.get(field_id)

    def parse_info_fields(self, header_lines: list[str]) -> dict[str, dict[str, str]]:
        """Parse INFO field definitions from header lines."""
        info_fields = {}
        info_pattern = re.compile(r'##INFO=<(.+)>')

        for line in header_lines:
            match = info_pattern.match(line)
            if match:
                field_def = self._parse_field_definition(match.group(1))
                if field_def:
                    info_fields[field_def['ID']] = {
                        k: v for k, v in field_def.items() if k != 'ID'
                    }

        return info_fields

    def parse_format_fields(self, header_lines: list[str]) -> dict[str, dict[str, str]]:
        """Parse FORMAT field definitions from header lines."""
        format_fields = {}
        format_pattern = re.compile(r'##FORMAT=<(.+)>')

        for line in header_lines:
            match = format_pattern.match(line)
            if match:
                field_def = self._parse_field_definition(match.group(1))
                if field_def:
                    format_fields[field_def['ID']] = {
                        k: v for k, v in field_def.items() if k != 'ID'
                    }

        return format_fields

    def parse_csq_header(self, header_lines: list[str]) -> list[str]:
        """Parse VEP CSQ field structure from header."""
        csq_pattern = re.compile(r'##INFO=<ID=CSQ,.+Description=".*Format:\s*([^"]+)">')

        for line in header_lines:
            match = csq_pattern.match(line)
            if match:
                format_string = match.group(1)
                return format_string.split('|')

        return []

    def _parse_field_definition(self, field_string: str) -> dict[str, str] | None:
        """Parse a field definition string like 'ID=AC,Number=A,Type=Integer,Description="..."'"""
        field_def = {}

        # Handle quoted descriptions that may contain commas
        parts = []
        current_part = ""
        in_quotes = False

        for char in field_string:
            if char == '"':
                in_quotes = not in_quotes
                current_part += char
            elif char == ',' and not in_quotes:
                parts.append(current_part)
                current_part = ""
            else:
                current_part += char

        if current_part:
            parts.append(current_part)

        for part in parts:
            if '=' in part:
                key, value = part.split('=', 1)
                # Remove quotes from description
                if key == 'Description' and value.startswith('"') and value.endswith('"'):
                    value = value[1:-1]
                field_def[key] = value

        return field_def if 'ID' in field_def else None


class VariantParser:
    """Parser for individual VCF variant records."""

    def __init__(self, header_parser: VCFHeaderParser | None = None, normalize: bool = False, human_genome: bool = True):
        self.header_parser = header_parser
        self.normalize = normalize
        self.human_genome = human_genome

    def parse_variant(self, variant, csq_fields: list[str], ann_fields: list[str] | None = None) -> list[VariantRecord]:
        """Parse a cyvcf2 variant into VariantRecord objects."""
        records = []
        n_alts = len(variant.ALT)

        for alt_idx, alt in enumerate(variant.ALT):
            if alt is None:
                continue

            info_dict = self._extract_info_for_alt(variant, alt_idx, n_alts)

            if self.human_genome:
                chrom = f"chr{variant.CHROM.replace('chr', '')}"
            else:
                chrom = variant.CHROM
            pos = variant.POS
            ref = variant.REF
            current_alt = alt

            original_pos = None
            original_ref = None
            original_alt = None
            was_normalized = False

            if self.normalize:
                norm_pos, norm_ref, norm_alts = normalize_variant(
                    chrom, pos, ref, [current_alt]
                )
                if norm_pos != pos or norm_ref != ref or norm_alts[0] != current_alt:
                    original_pos = pos
                    original_ref = ref
                    original_alt = current_alt
                    pos = norm_pos
                    ref = norm_ref
                    current_alt = norm_alts[0]
                    was_normalized = True

            record = VariantRecord(
                chrom=chrom,
                pos=pos,
                end_pos=info_dict.get('END'),
                ref=ref,
                alt=current_alt,
                qual=variant.QUAL if variant.QUAL != -1 else None,
                filter=variant.FILTER.split(';') if variant.FILTER and variant.FILTER != '.' else [],
                rs_id=variant.ID if variant.ID != '.' else None,
                info=info_dict,
                normalized=was_normalized,
                original_pos=original_pos,
                original_ref=original_ref,
                original_alt=original_alt
            )

            csq_value = variant.INFO.get('CSQ') if hasattr(variant, 'INFO') else None
            if csq_value and csq_fields:
                annotations = self._parse_csq(csq_value, csq_fields, alt)
                if annotations:
                    record.gene = annotations.get('SYMBOL')
                    record.consequence = annotations.get('Consequence')
                    record.impact = annotations.get('IMPACT')
                    record.hgvs_c = annotations.get('HGVSc')
                    record.hgvs_p = annotations.get('HGVSp')

            ann_value = variant.INFO.get('ANN') if hasattr(variant, 'INFO') else None
            if ann_value and ann_fields and record.gene is None:
                annotations = self._parse_ann(ann_value, ann_fields, alt)
                if annotations:
                    record.gene = annotations.get('Gene_Name')
                    record.consequence = annotations.get('Annotation')
                    record.impact = annotations.get('Annotation_Impact')
                    record.hgvs_c = annotations.get('HGVS.c')
                    record.hgvs_p = annotations.get('HGVS.p')
                    record.transcript = annotations.get('Feature_ID')

            if hasattr(variant, 'INFO'):
                record.af_gnomad = self._safe_float(info_dict.get('gnomAD_AF'))
                record.cadd_phred = self._safe_float(info_dict.get('CADD_PHRED'))
                record.clinvar_sig = info_dict.get('CLNSIG')

                if record.gene is None:
                    record.gene = info_dict.get('SYMBOL')
                if record.consequence is None:
                    record.consequence = info_dict.get('Consequence')
                if record.impact is None:
                    record.impact = info_dict.get('IMPACT')

            records.append(record)

        return records

    def _extract_info_for_alt(self, variant, alt_idx: int, n_alts: int) -> dict:
        """Extract INFO fields, indexing into Number=A/R/G fields for this ALT."""
        if not hasattr(variant, 'INFO'):
            return {}

        result = {}
        raw_info = dict(variant.INFO)

        for field_id, value in raw_info.items():
            if self.header_parser is None:
                result[field_id] = value
                continue

            field_meta = self.header_parser.get_info_field(field_id)
            if field_meta is None:
                result[field_id] = value
                continue

            number = field_meta.get('Number', '.')

            if number == 'A':
                result[field_id] = self._extract_number_a(value, alt_idx, n_alts)
            elif number == 'R':
                result[field_id] = self._extract_number_r(value, alt_idx, n_alts)
            elif number == 'G':
                result[field_id] = self._extract_number_g(value, alt_idx, n_alts)
            else:
                result[field_id] = value

        return result

    def _extract_number_a(self, value, alt_idx: int, n_alts: int):
        """Extract value for this ALT from a Number=A field."""
        if isinstance(value, (list, tuple)):
            if alt_idx < len(value):
                return value[alt_idx]
            return None
        if n_alts == 1:
            return value
        return value

    def _extract_number_r(self, value, alt_idx: int, n_alts: int):
        """Extract REF + this ALT values from a Number=R field."""
        if isinstance(value, (list, tuple)):
            if len(value) >= n_alts + 1:
                ref_val = value[0]
                alt_val = value[alt_idx + 1] if alt_idx + 1 < len(value) else None
                return [ref_val, alt_val]
            return value
        return value

    def _extract_number_g(self, value, alt_idx: int, n_alts: int):
        """Extract genotype likelihoods for biallelic (REF/ALT) from Number=G field."""
        if not isinstance(value, (list, tuple)):
            return value
        if n_alts == 1:
            return value

        idx_00 = 0
        idx_0alt = alt_idx + 1
        idx_altalt = ((alt_idx + 1) * (alt_idx + 2)) // 2 + (alt_idx + 1)

        result = []
        for idx in [idx_00, idx_0alt, idx_altalt]:
            if idx < len(value):
                result.append(value[idx])
            else:
                result.append(None)
        return result

    def _parse_csq(self, csq_value: str, fields: list[str], alt: str) -> dict[str, str] | None:
        """Parse VEP CSQ field, selecting worst consequence for this ALT."""
        impact_rank = {'HIGH': 0, 'MODERATE': 1, 'LOW': 2, 'MODIFIER': 3}
        best = None
        best_rank = 999

        for annotation in csq_value.split(','):
            values = annotation.split('|')
            if len(values) != len(fields):
                continue
            ann_dict = dict(zip(fields, values, strict=False))

            if ann_dict.get('Allele', '') != alt:
                continue

            rank = impact_rank.get(ann_dict.get('IMPACT', 'MODIFIER'), 3)
            if rank < best_rank:
                best = ann_dict
                best_rank = rank

        return best

    def _parse_ann(self, ann_value: str, fields: list[str], alt: str) -> dict[str, str] | None:
        """Parse SnpEff ANN field, selecting worst consequence for this ALT."""
        impact_rank = {'HIGH': 0, 'MODERATE': 1, 'LOW': 2, 'MODIFIER': 3}
        best = None
        best_rank = 999

        for annotation in ann_value.split(','):
            values = annotation.split('|')
            if len(values) < 4:
                continue

            ann_dict = {}
            for i, field in enumerate(fields):
                if i < len(values):
                    ann_dict[field] = values[i]

            allele = ann_dict.get('Allele', '')
            if allele and allele != alt:
                continue

            impact = ann_dict.get('Annotation_Impact', 'MODIFIER')
            rank = impact_rank.get(impact, 3)
            if rank < best_rank:
                best = ann_dict
                best_rank = rank

        return best

    def _safe_float(self, value) -> float | None:
        """Safely convert value to float."""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None


class VCFStreamingParser:
    """Streaming VCF parser that yields batches of VariantRecords."""

    DEFAULT_BATCH_SIZE = 10000

    def __init__(self, vcf_path: Path | str, batch_size: int | None = None, normalize: bool = False, human_genome: bool = True):
        self.vcf_path = Path(vcf_path) if isinstance(vcf_path, str) else vcf_path
        self.batch_size = batch_size if batch_size is not None else self.DEFAULT_BATCH_SIZE
        self.normalize = normalize
        self.human_genome = human_genome

        self._vcf: VCF | None = None
        self._closed = False
        self._variant_count = 0
        self._record_count = 0

        self.header_parser = VCFHeaderParser()
        self._init_vcf()

    def _init_vcf(self) -> None:
        """Initialize VCF reader and parse header."""
        self._vcf = VCF(str(self.vcf_path))
        self.header_parser.parse_from_vcf(self._vcf)

    @property
    def samples(self) -> list[str]:
        """Return sample names from VCF."""
        return self.header_parser.samples

    @property
    def variant_count(self) -> int:
        """Return count of VCF variant lines processed."""
        return self._variant_count

    @property
    def record_count(self) -> int:
        """Return count of records yielded (after multi-allelic decomposition)."""
        return self._record_count

    def iter_batches(self) -> Iterator[list[VariantRecord]]:
        """Iterate through VCF yielding batches of VariantRecords."""
        if self._vcf is None:
            self._init_vcf()

        variant_parser = VariantParser(self.header_parser, normalize=self.normalize, human_genome=self.human_genome)
        csq_fields = self.header_parser.csq_fields
        ann_fields = self.header_parser.ann_fields

        batch: list[VariantRecord] = []

        for variant in self._vcf:
            self._variant_count += 1
            records = variant_parser.parse_variant(variant, csq_fields, ann_fields)

            for record in records:
                self._record_count += 1
                batch.append(record)

                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []

        if batch:
            yield batch

    def close(self) -> None:
        """Close the VCF reader."""
        if self._vcf is not None:
            self._vcf.close()
            self._vcf = None
        self._closed = True

    def __enter__(self) -> "VCFStreamingParser":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
