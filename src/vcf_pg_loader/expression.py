"""Filter expression parser for echtvar-compatible syntax.

Converts expressions like:
    gnomad_af < 0.01 && clinvar_sig == 'Pathogenic'

To SQL WHERE clauses like:
    gnomad_af < 0.01 AND clinvar_sig = 'Pathogenic'
"""
import re
from dataclasses import dataclass


@dataclass
class ParseError:
    """Error encountered during expression parsing."""
    message: str
    position: int | None = None


class FilterExpressionParser:
    """Parse echtvar-style filter expressions to SQL WHERE clauses.

    Supported syntax:
    - Comparisons: <, <=, >, >=, ==, !=
    - Boolean operators: && (AND), || (OR)
    - NULL handling: IS NULL, IS NOT NULL
    - String literals: 'value' or "value"
    - Numeric literals: 0.01, 100, -5

    Examples:
        gnomad_af < 0.01
        gnomad_af < 0.01 && clinvar_sig == 'Pathogenic'
        gnomad_af < 0.01 || gnomad_af IS NULL
    """

    OPERATOR_MAP = {
        "&&": "AND",
        "||": "OR",
        "==": "=",
        "!=": "<>",
    }

    COMPARISON_OPS = {"<", "<=", ">", ">=", "==", "!=", "=", "<>"}

    KEYWORD_PATTERN = re.compile(
        r"\b(IS\s+NULL|IS\s+NOT\s+NULL)\b",
        re.IGNORECASE,
    )

    def parse(self, expr: str, available_fields: set[str]) -> str:
        """Parse an expression to SQL.

        Args:
            expr: The filter expression in echtvar syntax
            available_fields: Set of valid field names

        Returns:
            SQL WHERE clause (without the WHERE keyword)

        Raises:
            ValueError: If the expression is invalid
        """
        if not expr or not expr.strip():
            return "TRUE"

        errors = self.validate(expr, available_fields)
        if errors:
            raise ValueError(f"Invalid expression: {'; '.join(errors)}")

        sql = expr

        for echtvar_op, sql_op in self.OPERATOR_MAP.items():
            sql = sql.replace(echtvar_op, f" {sql_op} ")

        sql = re.sub(r"\s+", " ", sql).strip()

        return sql

    def validate(self, expr: str, available_fields: set[str]) -> list[str]:
        """Validate a filter expression.

        Args:
            expr: The filter expression to validate
            available_fields: Set of valid field names

        Returns:
            List of error messages (empty if valid)
        """
        errors = []

        if not expr or not expr.strip():
            return errors

        tokens = self._tokenize(expr)

        for token in tokens:
            if self._is_identifier(token):
                if token.upper() not in ("IS", "NULL", "NOT", "AND", "OR", "TRUE", "FALSE"):
                    if token not in available_fields:
                        errors.append(f"Unknown field: '{token}'")

        paren_depth = 0
        for char in expr:
            if char == "(":
                paren_depth += 1
            elif char == ")":
                paren_depth -= 1
            if paren_depth < 0:
                errors.append("Unbalanced parentheses")
                break

        if paren_depth != 0:
            errors.append("Unbalanced parentheses")

        in_string = False
        string_char = None
        for char in expr:
            if char in ('"', "'") and not in_string:
                in_string = True
                string_char = char
            elif char == string_char and in_string:
                in_string = False
                string_char = None

        if in_string:
            errors.append("Unclosed string literal")

        return errors

    def _tokenize(self, expr: str) -> list[str]:
        """Tokenize an expression into individual tokens."""
        tokens = []
        current = ""
        in_string = False
        string_char = None

        i = 0
        while i < len(expr):
            char = expr[i]

            if char in ('"', "'") and not in_string:
                if current:
                    tokens.append(current)
                    current = ""
                in_string = True
                string_char = char
                current += char
            elif char == string_char and in_string:
                current += char
                tokens.append(current)
                current = ""
                in_string = False
                string_char = None
            elif in_string:
                current += char
            elif char in " \t\n":
                if current:
                    tokens.append(current)
                    current = ""
            elif char in "()":
                if current:
                    tokens.append(current)
                    current = ""
                tokens.append(char)
            elif char in "<>=!":
                if current:
                    tokens.append(current)
                    current = ""
                op = char
                if i + 1 < len(expr) and expr[i + 1] in "=":
                    op += expr[i + 1]
                    i += 1
                tokens.append(op)
            elif char == "&" and i + 1 < len(expr) and expr[i + 1] == "&":
                if current:
                    tokens.append(current)
                    current = ""
                tokens.append("&&")
                i += 1
            elif char == "|" and i + 1 < len(expr) and expr[i + 1] == "|":
                if current:
                    tokens.append(current)
                    current = ""
                tokens.append("||")
                i += 1
            else:
                current += char

            i += 1

        if current:
            tokens.append(current)

        return tokens

    def _is_identifier(self, token: str) -> bool:
        """Check if a token is an identifier (field name or keyword)."""
        if not token:
            return False

        if token[0] in ('"', "'"):
            return False

        try:
            float(token)
            return False
        except ValueError:
            pass

        if token in self.COMPARISON_OPS or token in ("&&", "||", "(", ")"):
            return False

        return token.replace("_", "").isalnum()

    def extract_fields(self, expr: str) -> set[str]:
        """Extract field names from an expression.

        Args:
            expr: The filter expression

        Returns:
            Set of field names used in the expression
        """
        if not expr:
            return set()

        tokens = self._tokenize(expr)
        fields = set()

        for token in tokens:
            if self._is_identifier(token):
                if token.upper() not in ("IS", "NULL", "NOT", "AND", "OR", "TRUE", "FALSE"):
                    fields.add(token)

        return fields
