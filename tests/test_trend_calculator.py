"""
pytest test suite for analytics/trend_calculator.py
"""

import pandas as pd
import pytest

from analytics.trend_calculator import TrendCalculator


@pytest.fixture
def calc():
    return TrendCalculator(short_window=7, long_window=30)


class TestTrendCalculator:
    def test_improving_trend(self, calc):
        s = pd.Series([float(i) for i in range(40)])
        result = calc.calculate(s)
        assert result.direction == "improving"
        assert result.slope > 0

    def test_degrading_trend(self, calc):
        s = pd.Series([float(40 - i) for i in range(40)])
        result = calc.calculate(s)
        assert result.direction == "degrading"
        assert result.slope < 0

    def test_stable_trend(self, calc):
        s = pd.Series([95.0] * 40)
        result = calc.calculate(s)
        assert result.direction == "stable"
        assert abs(result.slope) < 0.01

    def test_rolling_mean_length(self, calc):
        s = pd.Series([95.0] * 20)
        result = calc.calculate(s)
        assert len(result.rolling_mean) == 20

    def test_pct_change_7d(self, calc):
        # Series goes from 80 to 100 over 10 observations
        s = pd.Series([80.0] + [85.0] * 8 + [100.0])
        result = calc.calculate(s)
        assert result.pct_change_7d is not None

    def test_pct_change_none_for_short_series(self, calc):
        s = pd.Series([95.0, 96.0, 94.0])
        result = calc.calculate(s)
        # 3 observations < short_window (7) → pct_change_7d should be None
        assert result.pct_change_7d is None
