"""VCF parsing functionality."""

import re
from math import comb

from .models import VariantRecord


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

    def parse_variant(self, variant, csq_fields: list[str]) -> list[VariantRecord]:
        """Parse a cyvcf2 variant into VariantRecord objects."""
        records = []

        # Handle multi-allelic: create one record per ALT
        for _i, alt in enumerate(variant.ALT):
            if alt is None:
                continue

            record = VariantRecord(
                chrom=f"chr{variant.CHROM.replace('chr', '')}",
                pos=variant.POS,
                end_pos=getattr(variant.INFO, 'END', None) if hasattr(variant, 'INFO') else None,
                ref=variant.REF,
                alt=alt,
                qual=variant.QUAL if variant.QUAL != -1 else None,
                filter=variant.FILTER.split(';') if variant.FILTER and variant.FILTER != '.' else [],
                rs_id=variant.ID if variant.ID != '.' else None,
                info=dict(variant.INFO) if hasattr(variant, 'INFO') else {}
            )

            # Extract VEP CSQ annotations if present
            if hasattr(variant, 'INFO') and 'CSQ' in variant.INFO and csq_fields:
                annotations = self._parse_csq(variant.INFO['CSQ'], csq_fields, alt)
                if annotations:
                    record.gene = annotations.get('SYMBOL')
                    record.consequence = annotations.get('Consequence')
                    record.impact = annotations.get('IMPACT')
                    record.hgvs_c = annotations.get('HGVSc')
                    record.hgvs_p = annotations.get('HGVSp')

            # Extract common annotation fields
            if hasattr(variant, 'INFO'):
                record.af_gnomad = self._safe_float(variant.INFO.get('gnomAD_AF'))
                record.cadd_phred = self._safe_float(variant.INFO.get('CADD_PHRED'))
                record.clinvar_sig = variant.INFO.get('CLNSIG')

            records.append(record)

        return records

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

            # Match to this ALT allele
            if ann_dict.get('Allele', '') != alt:
                continue

            rank = impact_rank.get(ann_dict.get('IMPACT', 'MODIFIER'), 3)
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
