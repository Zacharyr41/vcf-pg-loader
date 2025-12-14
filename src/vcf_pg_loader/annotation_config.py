"""Annotation field configuration for population databases.

This module provides configuration models for defining which fields to extract
from annotation VCFs (like gnomAD, ClinVar) and how to store them in PostgreSQL.

The configuration format is compatible with echtvar's JSON format:
https://github.com/brentp/echtvar
"""
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Literal


@dataclass
class AnnotationFieldConfig:
    """Configuration for a single annotation field.

    Attributes:
        field: Source VCF INFO field name (e.g., "AF", "AC")
        alias: Output column name in the annotation table (e.g., "gnomad_af")
        field_type: Data type - Integer, Float, or String
        missing_value: Value to use when field is missing (for numeric types)
        missing_string: Value to use when field is missing (for string types)
        multiplier: Multiplier for float precision (echtvar compatibility)
        description: Human-readable description of the field
    """
    field: str
    alias: str
    field_type: Literal["Integer", "Float", "String"] = "Integer"
    missing_value: int | float | None = None
    missing_string: str = "."
    multiplier: int = 1
    description: str = ""

    def to_sql_type(self) -> str:
        """Return the PostgreSQL type for this field."""
        if self.field_type == "Integer":
            return "INTEGER"
        elif self.field_type == "Float":
            return "REAL"
        else:
            return "TEXT"

    def is_special_field(self) -> bool:
        """Check if this is a special field like FILTER."""
        return self.field.upper() == "FILTER"


def load_field_config(path: Path) -> list[AnnotationFieldConfig]:
    """Load annotation field configuration from a JSON file.

    The JSON format is compatible with echtvar's configuration:
    ```json
    [
        {"field": "AC", "alias": "gnomad_ac"},
        {"field": "AF", "alias": "gnomad_af", "multiplier": 2000000},
        {"field": "FILTER", "alias": "gnomad_filter", "missing_string": "PASS"}
    ]
    ```

    Args:
        path: Path to the JSON configuration file

    Returns:
        List of AnnotationFieldConfig objects

    Raises:
        ValueError: If the configuration is invalid
        FileNotFoundError: If the file doesn't exist
    """
    with open(path) as f:
        raw_config = json.load(f)

    if not isinstance(raw_config, list):
        raise ValueError("Configuration must be a JSON array")

    fields = []
    for item in raw_config:
        if not isinstance(item, dict):
            raise ValueError("Each configuration item must be an object")

        if "field" not in item or "alias" not in item:
            raise ValueError("Each item must have 'field' and 'alias' keys")

        field_type = _infer_field_type(item)

        config = AnnotationFieldConfig(
            field=item["field"],
            alias=item["alias"],
            field_type=field_type,
            missing_value=item.get("missing_value"),
            missing_string=item.get("missing_string", "."),
            multiplier=item.get("multiplier", 1),
            description=item.get("description", ""),
        )
        fields.append(config)

    return fields


def _infer_field_type(item: dict) -> Literal["Integer", "Float", "String"]:
    """Infer the field type from configuration hints."""
    if "multiplier" in item and item.get("multiplier", 1) != 1:
        return "Float"

    if "missing_string" in item:
        return "String"

    field_name = item.get("field", "").upper()
    if field_name == "FILTER":
        return "String"
    if field_name in ("AF", "AF_POPMAX", "AF_CONTROLS_AND_BIOBANKS"):
        return "Float"

    return "Integer"


def validate_field_config(fields: list[AnnotationFieldConfig]) -> list[str]:
    """Validate a list of field configurations.

    Args:
        fields: List of field configurations to validate

    Returns:
        List of validation error messages (empty if valid)
    """
    errors = []
    aliases = set()

    for i, field_cfg in enumerate(fields):
        if not field_cfg.field:
            errors.append(f"Field {i}: 'field' cannot be empty")

        if not field_cfg.alias:
            errors.append(f"Field {i}: 'alias' cannot be empty")

        if field_cfg.alias in aliases:
            errors.append(f"Field {i}: duplicate alias '{field_cfg.alias}'")
        aliases.add(field_cfg.alias)

        if not field_cfg.alias.replace("_", "").isalnum():
            errors.append(
                f"Field {i}: alias '{field_cfg.alias}' contains invalid characters"
            )

        if field_cfg.multiplier <= 0:
            errors.append(f"Field {i}: multiplier must be positive")

    return errors


def config_to_dict(fields: list[AnnotationFieldConfig]) -> list[dict]:
    """Convert field configurations to JSON-serializable dictionaries."""
    result = []
    for field_cfg in fields:
        item = {
            "field": field_cfg.field,
            "alias": field_cfg.alias,
        }

        if field_cfg.field_type != "Integer":
            item["field_type"] = field_cfg.field_type

        if field_cfg.missing_value is not None:
            item["missing_value"] = field_cfg.missing_value

        if field_cfg.missing_string != ".":
            item["missing_string"] = field_cfg.missing_string

        if field_cfg.multiplier != 1:
            item["multiplier"] = field_cfg.multiplier

        if field_cfg.description:
            item["description"] = field_cfg.description

        result.append(item)

    return result
