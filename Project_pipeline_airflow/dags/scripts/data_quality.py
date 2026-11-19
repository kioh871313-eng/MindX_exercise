"""
data_quality.py
---------------
Data validation checks used by the Bronze DAG.
Silver and Gold validation runs directly in SQL inside the DAG files.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List, Optional, Dict

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    passed: bool = True
    errors: List[str] = field(default_factory=list)

    def fail(self, message: str) -> None:
        self.passed = False
        self.errors.append(message)
        logger.warning("Validation FAILED: %s", message)

    def summary(self) -> str:
        if self.passed:
            return "All validation checks passed."
        return "Validation failed with {} error(s): ".format(len(self.errors)) + " | ".join(self.errors)


def check_not_empty(df: pd.DataFrame, result: ValidationResult) -> None:
    if df.empty:
        result.fail("DataFrame is empty — no rows to process.")


def check_no_null_in_keys(
    df: pd.DataFrame,
    key_cols: List[str],
    result: ValidationResult,
) -> None:
    for col in key_cols:
        if col not in df.columns:
            result.fail("Key column '{}' is missing from DataFrame.".format(col))
            continue
        null_count = df[col].isnull().sum()
        if null_count > 0:
            result.fail("Key column '{}' has {} NULL value(s).".format(col, null_count))


def validate_bronze(df: pd.DataFrame, key_cols: List[str]) -> ValidationResult:
    """
    Light validation for the Bronze layer — accept raw data as-is but
    ensure basic structural integrity before inserting.
    """
    result = ValidationResult()
    check_not_empty(df, result)
    check_no_null_in_keys(df, key_cols, result)
    logger.info("Bronze validation complete. %s", result.summary())
    return result
