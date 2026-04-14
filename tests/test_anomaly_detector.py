"""
pytest test suite for analytics/anomaly_detector.py
"""

import numpy as np
import pandas as pd
import pytest

from analytics.anomaly_detector import AnomalyConfig, AnomalyDetector


@pytest.fixture
def detector():
    return AnomalyDetector(AnomalyConfig(zscore_threshold=3.0, min_observations=5))


class TestZScoreDetection:
    def test_no_anomalies_in_normal_series(self, detector):
        s = pd.Series([95.0, 96.0, 94.0, 95.5, 95.2, 94.8, 95.1])
        assert detector.detect(s) == []

    def test_detects_low_outlier(self, detector):
        values = [95.0] * 20 + [10.0]  # extreme low
        s = pd.Series(values)
        results = detector.detect(s)
        assert len(results) >= 1
        assert results[-1].value == 10.0
        assert results[-1].is_zscore is True

    def test_detects_high_outlier(self, detector):
        values = [50.0] * 20 + [200.0]  # extreme high
        s = pd.Series(values)
        results = detector.detect(s)
        assert any(r.value == 200.0 for r in results)

    def test_returns_empty_below_min_observations(self, detector):
        s = pd.Series([1.0, 2.0, 3.0])
        assert detector.detect(s) == []

    def test_severity_critical(self, detector):
        values = [50.0] * 50 + [0.0]  # very extreme
        s = pd.Series(values)
        results = detector.detect(s)
        assert results
        assert results[-1].severity in ("critical", "high")


class TestIQRDetection:
    def test_iqr_lower_bound(self):
        det = AnomalyDetector(AnomalyConfig(zscore_threshold=99.0, iqr_multiplier=1.5, min_observations=5))
        values = list(range(50, 100)) + [1]  # 1 is well below IQR lower
        s = pd.Series(values)
        results = det.detect(s)
        assert any(r.is_iqr for r in results)

    def test_iqr_upper_bound(self):
        det = AnomalyDetector(AnomalyConfig(zscore_threshold=99.0, iqr_multiplier=1.5, min_observations=5))
        values = list(range(50, 100)) + [300]  # 300 is well above IQR upper
        s = pd.Series(values)
        results = det.detect(s)
        assert any(r.is_iqr for r in results)


class TestRollingDetection:
    def test_rolling_detects_shift(self, detector):
        # Stable at ~50, then sudden shift to 10
        values = [50.0] * 30 + [10.0, 10.0, 10.0]
        s = pd.Series(values)
        results = detector.detect_rolling(s, window=10)
        assert len(results) >= 1

    def test_rolling_no_anomaly_in_stable_series(self, detector):
        s = pd.Series([95.0] * 40)
        results = detector.detect_rolling(s, window=10)
        assert results == []
