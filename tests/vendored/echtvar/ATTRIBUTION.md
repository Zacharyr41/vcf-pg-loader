# Echtvar Test Attribution

The test data generation scripts and test patterns in this directory are derived from
[echtvar](https://github.com/brentp/echtvar), a high-performance variant annotation tool.

## Original Work

- **Repository**: https://github.com/brentp/echtvar
- **Author**: Brent Pedersen
- **License**: MIT License (see LICENSE file in this directory)
- **Copyright**: Copyright (c) 2021 Brent Pedersen

## Modifications

The following modifications were made for integration with vcf-pg-loader:

1. Shell-based tests converted to Python pytest format
2. Rust/cargo build commands removed (replaced with Python equivalents)
3. Test assertions adapted for vcf-pg-loader's data structures
4. VCF generation scripts adapted from pure file writing to pytest fixtures

## Citation

If you use these tests or the underlying echtvar methodology, please cite:

> Echtvar: Really, truly rapid variant annotation and filtering
> https://github.com/brentp/echtvar
>
> Developed in the Jeroen De Ridder lab (https://www.deridderlab.nl/)

## Original Files Referenced

- `tests/make-vcf.py` → `generate_test_vcf.py`
- `tests/make-string-vcf.py` → `generate_string_vcf.py`
- `tests/big.sh` → `test_echtvar_compat.py`
- `tests/string.sh` → `test_echtvar_compat.py`
- `tests/check.py`, `tests/check-string.py` → validation logic in tests
