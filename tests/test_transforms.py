"""
Unit tests for the event transformation module.
"""

import pytest
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test_transforms")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def events_df(spark):
    """Create a sample events DataFrame matching the transform input schema."""
    schema = StructType(
        [
            StructField("event_id", StringType(), nullable=False),
            StructField("user_id", StringType(), nullable=False),
            StructField("event_type", StringType(), nullable=False),
            StructField("event_timestamp", StringType(), nullable=False),
            StructField("event_properties", StringType(), nullable=True),
            StructField("session_id", StringType(), nullable=True),
            StructField("platform", StringType(), nullable=True),
            StructField("app_version", StringType(), nullable=True),
            StructField("ip_address", StringType(), nullable=True),
            StructField("user_agent", StringType(), nullable=True),
            StructField("_extracted_at", TimestampType(), nullable=True),
        ]
    )
    data = [
        ("e1", "u1", "page_view", "2024-01-15T10:00:00", None, "s1", "web", "2.0", "1.2.3.4", "Mozilla/5.0", datetime(2024, 1, 15, 10, 0)),
        ("e2", "u1", "click", "2024-01-15T10:05:00", None, "s1", "web", "2.0", "1.2.3.4", "Mozilla/5.0", datetime(2024, 1, 15, 10, 5)),
        ("e3", "u1", "page_view", "2024-01-15T11:00:00", None, "s2", "web", "2.0", "1.2.3.4", "Mozilla/5.0", datetime(2024, 1, 15, 11, 0)),
        ("e4", "u2", "signup", "2024-01-15T09:00:00", None, "s3", "ios", "2.1", "5.6.7.8", "iPhone App", datetime(2024, 1, 15, 9, 0)),
        ("e1", "u1", "page_view", "2024-01-15T10:00:00", None, "s1", "web", "2.0", "1.2.3.4", "Mozilla/5.0", datetime(2024, 1, 15, 10, 1)),  # duplicate
    ]
    return spark.createDataFrame(data, schema)


class TestDeduplication:
    def test_removes_duplicate_event_ids(self, spark, events_df):
        """Deduplication should keep only the earliest record per event_id."""
        import sys
        sys.path.insert(0, "spark-jobs/etl")
        from transform_events import deduplicate

        result = deduplicate(events_df)
        assert result.count() == 4  # e1 appears twice, should become 1

    def test_keeps_unique_events(self, spark, events_df):
        """Deduplication should not remove unique events."""
        import sys
        sys.path.insert(0, "spark-jobs/etl")
        from transform_events import deduplicate

        result = deduplicate(events_df)
        event_ids = [row.event_id for row in result.select("event_id").collect()]
        assert "e2" in event_ids
        assert "e3" in event_ids
        assert "e4" in event_ids


class TestUDFs:
    def test_anonymise_ip(self):
        """IP anonymisation should produce a deterministic 16-char hex hash."""
        import sys
        sys.path.insert(0, "spark-jobs/etl")
        from transform_events import _anonymise_ip

        result = _anonymise_ip("1.2.3.4")
        assert result is not None
        assert len(result) == 16
        assert all(c in "0123456789abcdef" for c in result)

        # Deterministic
        assert _anonymise_ip("1.2.3.4") == result

    def test_anonymise_ip_none(self):
        """IP anonymisation should handle None gracefully."""
        import sys
        sys.path.insert(0, "spark-jobs/etl")
        from transform_events import _anonymise_ip

        assert _anonymise_ip(None) is None

    def test_classify_device_mobile(self):
        """Device classifier should identify mobile user agents."""
        import sys
        sys.path.insert(0, "spark-jobs/etl")
        from transform_events import _classify_device

        assert _classify_device("Mozilla/5.0 (iPhone; CPU)") == "mobile"
        assert _classify_device("Android WebView") == "mobile"

    def test_classify_device_desktop(self):
        """Device classifier should default to desktop for standard UAs."""
        import sys
        sys.path.insert(0, "spark-jobs/etl")
        from transform_events import _classify_device

        assert _classify_device("Mozilla/5.0 (Windows NT 10.0)") == "desktop"

    def test_classify_device_none(self):
        """Device classifier should return 'unknown' for None."""
        import sys
        sys.path.insert(0, "spark-jobs/etl")
        from transform_events import _classify_device

        assert _classify_device(None) == "unknown"


class TestSessionization:
    def test_assigns_session_ids(self, spark, events_df):
        """Sessionization should assign computed_session_id to each event."""
        import sys
        sys.path.insert(0, "spark-jobs/etl")
        from transform_events import (
            deduplicate,
            add_window_functions,
            sessionize,
        )

        deduped = deduplicate(events_df)
        parsed = (
            deduped.withColumn("event_ts", F.to_timestamp("event_timestamp"))
            .withColumn("event_date", F.to_date("event_ts"))
        )
        windowed = add_window_functions(parsed)
        result = sessionize(windowed)

        assert "computed_session_id" in result.columns
        session_ids = [
            row.computed_session_id
            for row in result.select("computed_session_id").distinct().collect()
        ]
        assert len(session_ids) > 0

    def test_new_session_after_30min_gap(self, spark, events_df):
        """Events more than 30 minutes apart should be in different sessions."""
        import sys
        sys.path.insert(0, "spark-jobs/etl")
        from transform_events import (
            deduplicate,
            add_window_functions,
            sessionize,
        )

        deduped = deduplicate(events_df)
        parsed = (
            deduped.withColumn("event_ts", F.to_timestamp("event_timestamp"))
            .withColumn("event_date", F.to_date("event_ts"))
        )
        windowed = add_window_functions(parsed)
        result = sessionize(windowed)

        # u1 has events at 10:00, 10:05 (same session) and 11:00 (new session)
        u1_sessions = (
            result.filter(F.col("user_id") == "u1")
            .select("computed_session_id")
            .distinct()
            .count()
        )
        assert u1_sessions == 2


class TestQualityAssertions:
    def test_assert_quality_passes_on_valid_data(self, spark, events_df):
        """Quality assertions should pass on valid data."""
        import sys
        sys.path.insert(0, "spark-jobs/etl")
        from transform_events import assert_quality

        parsed = events_df.withColumn("event_ts", F.to_timestamp("event_timestamp"))
        # Should not raise
        assert_quality(parsed, "test_valid")

    def test_assert_quality_fails_on_empty_df(self, spark):
        """Quality assertions should fail on empty DataFrames."""
        import sys
        sys.path.insert(0, "spark-jobs/etl")
        from transform_events import assert_quality

        schema = StructType(
            [
                StructField("event_id", StringType()),
                StructField("user_id", StringType()),
                StructField("event_ts", TimestampType()),
            ]
        )
        empty_df = spark.createDataFrame([], schema)

        with pytest.raises(ValueError, match="DataFrame is empty"):
            assert_quality(empty_df, "test_empty")
