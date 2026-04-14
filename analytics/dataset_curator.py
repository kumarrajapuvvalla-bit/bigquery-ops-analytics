"""
dataset_curator.py
──────────────────
Traceable dataset curation utilities.

Handles:
  - Schema validation (column presence, types, nullability)
  - Lineage recording (source → transformation → output)
  - Data quality scoring (completeness, uniqueness, freshness)
  - BigQuery table metadata writing
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

import pandas as pd

log = logging.getLogger(__name__)


@dataclass
class ColumnSpec:
    name:       str
    dtype:      str       # e.g., "int64", "float64", "object", "datetime64[ns]"
    nullable:   bool = True
    unique:     bool = False


@dataclass
class LineageRecord:
    source:         str
    transformation: str
    output:         str
    run_ts:         datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    row_count:      int = 0
    checksum:       str = ""


@dataclass
class QualityScore:
    completeness:  float   # fraction of non-null values
    uniqueness:    float   # fraction of unique values (for key columns)
    freshness_ok:  bool    # most recent record within max_age_hours
    overall:       float   # weighted composite


class DatasetCurator:
    """Validates, curates, and records lineage for a pandas DataFrame."""

    def __init__(
        self,
        schema:          list[ColumnSpec],
        max_age_hours:   int = 25,
        ts_column:       Optional[str] = None,
    ) -> None:
        self.schema        = schema
        self.max_age_hours = max_age_hours
        self.ts_column     = ts_column
        self._lineage:     list[LineageRecord] = []

    # ── Public API ───────────────────────────────────────────────────────

    def validate(self, df: pd.DataFrame) -> list[str]:
        """
        Validate df against the schema.

        Returns:
            List of validation error messages. Empty list = pass.
        """
        errors: list[str] = []
        for col in self.schema:
            if col.name not in df.columns:
                errors.append(f"Missing column: {col.name}")
                continue
            if not col.nullable and df[col.name].isnull().any():
                null_count = int(df[col.name].isnull().sum())
                errors.append(
                    f"Column '{col.name}' is not nullable but has {null_count} nulls."
                )
            if col.unique and df[col.name].duplicated().any():
                dup_count = int(df[col.name].duplicated().sum())
                errors.append(
                    f"Column '{col.name}' must be unique but has {dup_count} duplicates."
                )
        return errors

    def score(self, df: pd.DataFrame) -> QualityScore:
        """Compute a data quality score for the DataFrame."""
        total_cells = df.shape[0] * df.shape[1]
        completeness = (
            1.0 - df.isnull().sum().sum() / total_cells
            if total_cells > 0 else 1.0
        )

        key_cols = [c for c in self.schema if c.unique]
        if key_cols and df.shape[0] > 0:
            uniqueness = min(
                df[c.name].nunique() / df.shape[0]
                for c in key_cols
                if c.name in df.columns
            )
        else:
            uniqueness = 1.0

        freshness_ok = True
        if self.ts_column and self.ts_column in df.columns:
            latest = pd.to_datetime(df[self.ts_column]).max()
            if pd.notnull(latest):
                age_hours = (
                    datetime.now(timezone.utc)
                    - latest.to_pydatetime().replace(tzinfo=timezone.utc)
                ).total_seconds() / 3600
                freshness_ok = age_hours <= self.max_age_hours

        overall = (
            0.5 * completeness
            + 0.3 * uniqueness
            + 0.2 * (1.0 if freshness_ok else 0.0)
        )

        return QualityScore(
            completeness  = round(completeness, 4),
            uniqueness    = round(uniqueness, 4),
            freshness_ok  = freshness_ok,
            overall       = round(overall, 4),
        )

    def record_lineage(
        self,
        df:             pd.DataFrame,
        source:         str,
        transformation: str,
        output:         str,
    ) -> LineageRecord:
        """Record a lineage entry for this transformation step."""
        checksum = hashlib.md5(  # noqa: S324  (non-security use)
            pd.util.hash_pandas_object(df, index=True).values.tobytes()
        ).hexdigest()
        record = LineageRecord(
            source         = source,
            transformation = transformation,
            output         = output,
            row_count      = len(df),
            checksum       = checksum,
        )
        self._lineage.append(record)
        log.info(
            "Lineage: %s → %s → %s  rows=%d  checksum=%s",
            source, transformation, output, record.row_count, record.checksum,
        )
        return record

    @property
    def lineage(self) -> list[LineageRecord]:
        return list(self._lineage)
