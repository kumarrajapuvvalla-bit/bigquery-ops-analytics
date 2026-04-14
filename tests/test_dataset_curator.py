"""
pytest test suite for analytics/dataset_curator.py
"""

import pandas as pd
import pytest

from analytics.dataset_curator import ColumnSpec, DatasetCurator


SCHEMA = [
    ColumnSpec(name="event_id",    dtype="object",  nullable=False, unique=True),
    ColumnSpec(name="cluster",     dtype="object",  nullable=False),
    ColumnSpec(name="score",       dtype="float64", nullable=True),
    ColumnSpec(name="event_ts",    dtype="object",  nullable=False),
]


@pytest.fixture
def curator():
    return DatasetCurator(schema=SCHEMA, ts_column="event_ts")


@pytest.fixture
def good_df():
    return pd.DataFrame({
        "event_id": ["e1", "e2", "e3"],
        "cluster":  ["prod-eks", "prod-eks", "staging-eks"],
        "score":    [95.0, 92.5, 88.0],
        "event_ts": pd.date_range("2026-04-14", periods=3, freq="h"),
    })


class TestValidation:
    def test_valid_df_passes(self, curator, good_df):
        errors = curator.validate(good_df)
        assert errors == []

    def test_missing_column(self, curator):
        df = pd.DataFrame({"event_id": ["e1"], "cluster": ["eks"]})
        errors = curator.validate(df)
        assert any("score" in e or "event_ts" in e for e in errors)

    def test_null_in_non_nullable(self, curator, good_df):
        good_df.loc[0, "cluster"] = None
        errors = curator.validate(good_df)
        assert any("cluster" in e for e in errors)

    def test_duplicate_unique_column(self, curator, good_df):
        good_df.loc[1, "event_id"] = "e1"  # duplicate
        errors = curator.validate(good_df)
        assert any("event_id" in e for e in errors)


class TestQualityScore:
    def test_perfect_score(self, curator, good_df):
        qs = curator.score(good_df)
        assert qs.completeness == 1.0
        assert qs.uniqueness   == 1.0
        assert qs.overall      > 0.8

    def test_completeness_drops_with_nulls(self, curator, good_df):
        good_df.loc[0, "score"] = None
        qs = curator.score(good_df)
        assert qs.completeness < 1.0


class TestLineage:
    def test_lineage_recorded(self, curator, good_df):
        record = curator.record_lineage(
            good_df,
            source="ops_raw.fleet_events",
            transformation="stg_fleet_events",
            output="ops_analytics.stg_fleet_events",
        )
        assert record.row_count == 3
        assert record.checksum != ""
        assert len(curator.lineage) == 1

    def test_lineage_checksum_changes_on_data_change(self, curator, good_df):
        r1 = curator.record_lineage(good_df, "s", "t", "o")
        good_df.loc[0, "score"] = 0.0
        r2 = curator.record_lineage(good_df, "s", "t", "o")
        assert r1.checksum != r2.checksum
