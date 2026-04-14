"""
anomal_detector.py
──────────────────
Python anomaly detection utilities used by the Airflow DAGs
and pytest test suite.

Implements two complementary strategies:
  1. Z-score (parametric) — flags observations > N standard deviations from
     a rolling mean. Best for normally distributed metrics.
  2. IQR (non-parametric) — flags observations outside Q1 - k*IQR to
     Q3 + k*IQR. More robust to skewed distributions.

The same logic is mirrored in BigQuery SQL (int_anomaly_candidates.sql)
for in-warehouse execution at scale.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Sequence

import numpy as np
import pandas as pd


@dataclass
class AnomalyConfig:
    zscore_threshold: float = 3.0
    iqr_multiplier:   float = 1.5
    min_observations: int   = 10      # skip detection with insufficient history
    lookback_days:    int   = 30


@dataclass
class AnomalyResult:
    index:          int
    timestamp:      Optional[pd.Timestamp]
    value:          float
    z_score:        float
    rolling_mean:   float
    rolling_std:    float
    iqr_lower:      float
    iqr_upper:      float
    is_zscore:      bool
    is_iqr:         bool
    is_anomaly:     bool
    severity:       str   # low | medium | high | critical


class AnomalyDetector:
    """Detects anomalies in a time-series of numeric values."""

    def __init__(self, config: Optional[AnomalyConfig] = None) -> None:
        self.config = config or AnomalyConfig()

    # ── Public API ───────────────────────────────────────────────────────

    def detect(self, series: pd.Series) -> list[AnomalyResult]:
        """
        Detect anomalies in a pandas Series.

        Args:
            series: Numeric time-series. Index can be datetime or integer.

        Returns:
            List of AnomalyResult for all detected anomalies (not normal obs).
        """
        if len(series) < self.config.min_observations:
            return []

        values  = series.values.astype(float)
        mean    = np.nanmean(values)
        std     = np.nanstd(values)
        q1, q3  = np.nanpercentile(values, [25, 75])
        iqr     = q3 - q1

        results = []
        for i, v in enumerate(values):
            if np.isnan(v):
                continue

            z        = (v - mean) / std if std > 0 else 0.0
            is_z     = abs(z) >= self.config.zscore_threshold
            is_iqr   = (
                v < q1 - self.config.iqr_multiplier * iqr
                or v > q3 + self.config.iqr_multiplier * iqr
            )
            is_anom  = is_z or is_iqr

            if not is_anom:
                continue

            ts = series.index[i] if isinstance(series.index, pd.DatetimeIndex) else None
            results.append(AnomalyResult(
                index        = i,
                timestamp    = ts,
                value        = v,
                z_score      = round(z, 4),
                rolling_mean = round(mean, 4),
                rolling_std  = round(std, 4),
                iqr_lower    = round(q1 - self.config.iqr_multiplier * iqr, 4),
                iqr_upper    = round(q3 + self.config.iqr_multiplier * iqr, 4),
                is_zscore    = is_z,
                is_iqr       = is_iqr,
                is_anomaly   = True,
                severity     = self._severity(abs(z)),
            ))

        return results

    def detect_rolling(
        self,
        series: pd.Series,
        window: int = 24,
    ) -> list[AnomalyResult]:
        """
        Detect anomalies using a rolling window (more sensitive to local shifts).

        Args:
            series: Numeric time-series.
            window: Number of observations in the rolling window.
        """
        if len(series) < self.config.min_observations:
            return []

        values  = series.values.astype(float)
        results = []

        for i in range(window, len(values)):
            window_vals = values[i - window:i]
            v           = values[i]
            if np.isnan(v):
                continue

            mean = np.nanmean(window_vals)
            std  = np.nanstd(window_vals)
            q1, q3 = np.nanpercentile(window_vals, [25, 75])
            iqr  = q3 - q1

            z        = (v - mean) / std if std > 0 else 0.0
            is_z     = abs(z) >= self.config.zscore_threshold
            is_iqr   = (
                v < q1 - self.config.iqr_multiplier * iqr
                or v > q3 + self.config.iqr_multiplier * iqr
            )
            is_anom  = is_z or is_iqr

            if not is_anom:
                continue

            ts = series.index[i] if isinstance(series.index, pd.DatetimeIndex) else None
            results.append(AnomalyResult(
                index        = i,
                timestamp    = ts,
                value        = v,
                z_score      = round(z, 4),
                rolling_mean = round(mean, 4),
                rolling_std  = round(std, 4),
                iqr_lower    = round(q1 - self.config.iqr_multiplier * iqr, 4),
                iqr_upper    = round(q3 + self.config.iqr_multiplier * iqr, 4),
                is_zscore    = is_z,
                is_iqr       = is_iqr,
                is_anomaly   = True,
                severity     = self._severity(abs(z)),
            ))

        return results

    # ── Private helpers ──────────────────────────────────────────────────

    @staticmethod
    def _severity(abs_z: float) -> str:
        if abs_z >= 5:   return "critical"
        if abs_z >= 4:   return "high"
        if abs_z >= 3:   return "medium"
        return "low"
