# Annotation Field Configuration

This document describes the JSON configuration format for defining which fields
to extract from annotation VCFs and how to store them in PostgreSQL.

The format is compatible with [echtvar's configuration](https://github.com/brentp/echtvar).

## Configuration Format

A configuration file is a JSON array of field definitions:

```json
[
  {"field": "AC", "alias": "gnomad_ac"},
  {"field": "AF", "alias": "gnomad_af", "multiplier": 2000000},
  {"field": "FILTER", "alias": "gnomad_filter", "missing_string": "PASS"}
]
```

## Field Properties

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `field` | string | Yes | VCF INFO field name (e.g., "AF", "AC") |
| `alias` | string | Yes | Output column name in PostgreSQL |
| `multiplier` | integer | No | Multiplier for float precision (echtvar compat) |
| `missing_value` | number | No | Value for missing numeric fields |
| `missing_string` | string | No | Value for missing string fields (default: ".") |
| `description` | string | No | Human-readable field description |

## Type Inference

Field types are automatically inferred:

1. If `multiplier` is set (and ≠ 1): `Float`
2. If `missing_string` is set: `String`
3. If field name is "FILTER": `String`
4. If field name is "AF", "AF_POPMAX", etc.: `Float`
5. Otherwise: `Integer`

## SQL Type Mapping

| Inferred Type | PostgreSQL Type |
|---------------|-----------------|
| Integer | INTEGER |
| Float | REAL |
| String | TEXT |

## Examples

### gnomAD Configuration

```json
[
  {"field": "AC", "alias": "gnomad_ac"},
  {"field": "AN", "alias": "gnomad_an"},
  {"field": "AF", "alias": "gnomad_af", "multiplier": 2000000},
  {"field": "nhomalt", "alias": "gnomad_nhomalt"},
  {"field": "AC_popmax", "alias": "gnomad_ac_popmax"},
  {"field": "AN_popmax", "alias": "gnomad_an_popmax"},
  {"field": "AF_popmax", "alias": "gnomad_af_popmax", "multiplier": 2000000},
  {"field": "FILTER", "alias": "gnomad_filter", "missing_string": "PASS"}
]
```

### ClinVar Configuration

```json
[
  {"field": "CLNSIG", "alias": "clinvar_sig", "missing_string": "."},
  {"field": "CLNREVSTAT", "alias": "clinvar_revstat", "missing_string": "."},
  {"field": "CLNDN", "alias": "clinvar_disease", "missing_string": "."},
  {"field": "CLNVC", "alias": "clinvar_variant_type", "missing_string": "."}
]
```

### dbSNP Configuration

```json
[
  {"field": "RS", "alias": "dbsnp_id"},
  {"field": "CAF", "alias": "dbsnp_maf", "multiplier": 1000000}
]
```

## Validation

Configurations are validated at load time:

- `field` and `alias` are required
- `alias` must be unique across all fields
- `alias` must contain only alphanumeric characters and underscores
- `multiplier` must be positive (if specified)

## Python API

```python
from pathlib import Path
from vcf_pg_loader.annotation_config import (
    load_field_config,
    validate_field_config,
    AnnotationFieldConfig,
)

# Load from JSON file
fields = load_field_config(Path("gnomad.json"))

# Validate configuration
errors = validate_field_config(fields)
if errors:
    print("Validation errors:", errors)

# Create programmatically
field = AnnotationFieldConfig(
    field="AF",
    alias="gnomad_af",
    field_type="Float",
    missing_value=-1.0,
)
```

## Special Fields

### FILTER

The VCF FILTER field is handled specially:

- Extracted from `variant.FILTER`, not INFO
- Returns "PASS" if filter is None (variant passed all filters)
- Always stored as TEXT type

```json
{"field": "FILTER", "alias": "gnomad_filter", "missing_string": "PASS"}
```

### Number=A Fields

Fields with `Number=A` in the VCF header (one value per alternate allele) are
automatically decomposed. The value corresponding to each ALT allele is extracted.

## echtvar Compatibility

This configuration format is compatible with echtvar's JSON format, allowing you
to reuse existing echtvar configurations:

```bash
# Use existing echtvar config
vcf-pg-loader load-annotation gnomad.vcf.gz \
  --name gnomad_v3 \
  --config examples/gnomad.v3.1.2.json
```

Note: The `multiplier` field is preserved for compatibility but does not affect
the stored values in PostgreSQL (values are stored as native floats).
