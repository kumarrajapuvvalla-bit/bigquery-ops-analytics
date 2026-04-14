"""
trend_calculator.py
───────────────────
Rolling average, exponential smoothing, and linear trend
utilities for fleet and ops time-series data.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd


@dataclass
class TrendResult:
    series:             pd.Series
    rolling_mean:       pd.Series
    ewm_mean:           pd.Series
    slope:              float          # linear trend slope (units per period)
    direction:          str            # improving | stable | degrading
    pct_change_7d:      Optional[float]
    pct_change_30d:     Optional[float]


class TrendCalculator:
    """Computes rolling averages and trend direction for a numeric series."""

    def __init__(self, short_window: int = 7, long_window: int = 30) -> None:
        self.short_window = short_window
        self.long_window  = long_window

    def calculate(self, series: pd.Series) -> TrendResult:
        """
        Compute trend statistics for a time-series.

        Args:
            series: Numeric pandas Series (daily or hourly).

        Returns:
            TrendResult with rolling means, slope, direction, pct changes.
        """
        values       = series.values.astype(float)
        rolling_mean = pd.Series(values).rolling(self.short_window, min_periods=1).mean()
        ewm_mean     = pd.Series(values).ewm(span=self.short_window, adjust=False).mean()

        # Linear regression slope
        x     = np.arange(len(values))
        slope = float(np.polyfit(x[~np.isnan(values)], values[~np.isnan(values)], 1)[0])

        direction = (
            "improving"  if slope >  0.1
            else "degrading" if slope < -0.1
            else "stable"
        )

        pct_7d  = self._pct_change(values, self.short_window)
        pct_30d = self._pct_change(values, self.long_window)

        return TrendResult(
            series         = series,
            rolling_mean   = rolling_mean,
            ewm_mean       = ewm_mean,
            slope          = round(slope, 6),
            direction      = direction,
            pct_change_7d  = pct_7d,
            pct_change_30d = pct_30d,
        )

    @staticmethod
    def _pct_change(values: np.ndarray, periods: int) -> Optional[float]:
        if len(values) <= periods:
            return None
        old = values[-periods - 1]
        new = values[-1]
        if old == 0 or np.isnan(old) or np.isnan(new):
            return None
        return round((new - old) / abs(old) * 100, 2)
