"""
Unit tests for the custom data quality framework.
"""

import pytest

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    DoubleType,
)


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test_data_quality")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def sample_df(spark):
    """Create a sample DataFrame for testing."""
    schema = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField("email", StringType(), nullable=True),
            StructField("age", IntegerType(), nullable=True),
            StructField("score", DoubleType(), nullable=True),
        ]
    )
    data = [
        (1, "Alice", "alice@example.com", 30, 85.5),
        (2, "Bob", "bob@example.com", 25, 92.0),
        (3, None, "charlie@example.com", 35, 78.3),
        (4, "Diana", None, 28, 95.1),
        (5, "Eve", "invalid-email", 22, 88.7),
        (1, "Alice Dup", "alice2@example.com", 30, 85.5),  # duplicate id
    ]
    return spark.createDataFrame(data, schema)


class TestNullCheck:
    def test_passes_when_no_nulls(self, sample_df):
        """Null check should pass on a column with no nulls."""
        import sys
        sys.path.insert(0, "spark-jobs/utils")
        from data_quality import Check, Severity

        check = Check.null_check("id", max_null_pct=0.0, severity=Severity.FAIL)
        result = check.validator(sample_df)
        assert result.passed is True

    def test_fails_when_nulls_exceed_threshold(self, sample_df):
        """Null check should fail when null percentage exceeds threshold."""
        import sys
        sys.path.insert(0, "spark-jobs/utils")
        from data_quality import Check, Severity

        check = Check.null_check("name", max_null_pct=0.0, severity=Severity.FAIL)
        result = check.validator(sample_df)
        assert result.passed is False

    def test_passes_when_nulls_within_threshold(self, sample_df):
        """Null check should pass when nulls are within tolerance."""
        import sys
        sys.path.insert(0, "spark-jobs/utils")
        from data_quality import Check, Severity

        check = Check.null_check("name", max_null_pct=0.5, severity=Severity.WARN)
        result = check.validator(sample_df)
        assert result.passed is True


class TestUniqueCheck:
    def test_fails_with_duplicates(self, sample_df):
        """Unique check should fail when duplicates exist."""
        import sys
        sys.path.insert(0, "spark-jobs/utils")
        from data_quality import Check, Severity

        check = Check.unique_check("id", severity=Severity.FAIL)
        result = check.validator(sample_df)
        assert result.passed is False
        assert result.metric_value == 1  # one duplicate

    def test_passes_without_duplicates(self, sample_df):
        """Unique check should pass on a column with all unique values."""
        import sys
        sys.path.insert(0, "spark-jobs/utils")
        from data_quality import Check, Severity

        check = Check.unique_check("email", severity=Severity.FAIL)
        result = check.validator(sample_df)
        # email has a null which is distinct, so all 6 are distinct
        assert result.passed is True


class TestRangeCheck:
    def test_passes_within_range(self, sample_df):
        """Range check should pass when all values are within bounds."""
        import sys
        sys.path.insert(0, "spark-jobs/utils")
        from data_quality import Check, Severity

        check = Check.range_check("age", min_val=18, max_val=100, severity=Severity.FAIL)
        result = check.validator(sample_df)
        assert result.passed is True

    def test_fails_out_of_range(self, sample_df):
        """Range check should fail when values exceed bounds."""
        import sys
        sys.path.insert(0, "spark-jobs/utils")
        from data_quality import Check, Severity

        check = Check.range_check("age", min_val=25, max_val=33, severity=Severity.FAIL)
        result = check.validator(sample_df)
        assert result.passed is False


class TestRegexCheck:
    def test_fails_on_invalid_pattern(self, sample_df):
        """Regex check should fail when values don't match the pattern."""
        import sys
        sys.path.insert(0, "spark-jobs/utils")
        from data_quality import Check, Severity

        check = Check.regex_check(
            "email",
            pattern=r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$",
            max_violation_pct=0.0,
            severity=Severity.FAIL,
        )
        result = check.validator(sample_df)
        # "invalid-email" does not match
        assert result.passed is False


class TestQualityRunner:
    def test_runner_produces_report(self, sample_df):
        """Runner should execute all checks and produce a report."""
        import sys
        sys.path.insert(0, "spark-jobs/utils")
        from data_quality import Check, DataQualityRunner, Severity

        runner = DataQualityRunner()
        runner.add_check(Check.null_check("id", severity=Severity.FAIL))
        runner.add_check(Check.unique_check("id", severity=Severity.WARN))
        runner.add_check(Check.range_check("age", min_val=0, max_val=200, severity=Severity.FAIL))

        report = runner.run(sample_df)

        assert len(report.results) == 3
        assert report.passed is True  # unique check is WARN severity, so overall passes

    def test_runner_fails_on_fail_severity(self, sample_df):
        """Runner report should reflect FAIL severity check failures."""
        import sys
        sys.path.insert(0, "spark-jobs/utils")
        from data_quality import Check, DataQualityRunner, Severity

        runner = DataQualityRunner()
        runner.add_check(Check.unique_check("id", severity=Severity.FAIL))

        report = runner.run(sample_df)

        assert report.passed is False
        assert len(report.failures) == 1
