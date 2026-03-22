"""
Custom data quality framework for PySpark DataFrames.

Provides reusable quality checks including null checks, uniqueness,
range validation, referential integrity, schema compatibility,
and custom SQL-based checks. Results are aggregated into a report
with configurable fail/warn thresholds.

Usage:
    from data_quality import DataQualityRunner, Check, Severity

    runner = DataQualityRunner(spark, df)
    runner.add_null_check("event_id", severity=Severity.CRITICAL)
    runner.add_uniqueness_check("event_id")
    runner.add_range_check("revenue", min_val=0, max_val=100000)
    report = runner.run()
    report.raise_on_failure()
"""

import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------
class Severity(str, Enum):
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


class CheckStatus(str, Enum):
    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    ERROR = "error"


@dataclass
class CheckResult:
    """Result of a single data quality check."""
    check_name: str
    check_type: str
    column: Optional[str]
    severity: Severity
    status: CheckStatus
    expected: Any
    actual: Any
    details: str = ""
    duration_ms: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "check_name": self.check_name,
            "check_type": self.check_type,
            "column": self.column,
            "severity": self.severity.value,
            "status": self.status.value,
            "expected": str(self.expected),
            "actual": str(self.actual),
            "details": self.details,
            "duration_ms": self.duration_ms,
        }


@dataclass
class QualityReport:
    """Aggregated data quality report."""
    table_name: str
    total_records: int
    checks: List[CheckResult] = field(default_factory=list)
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None

    @property
    def passed(self) -> int:
        return sum(1 for c in self.checks if c.status == CheckStatus.PASSED)

    @property
    def failed(self) -> int:
        return sum(1 for c in self.checks if c.status == CheckStatus.FAILED)

    @property
    def warnings(self) -> int:
        return sum(1 for c in self.checks if c.status == CheckStatus.WARNING)

    @property
    def critical_failures(self) -> int:
        return sum(
            1 for c in self.checks
            if c.status == CheckStatus.FAILED and c.severity == Severity.CRITICAL
        )

    @property
    def is_success(self) -> bool:
        return self.critical_failures == 0

    @property
    def duration_seconds(self) -> float:
        end = self.end_time or time.time()
        return round(end - self.start_time, 2)

    def summary(self) -> Dict[str, Any]:
        return {
            "table_name": self.table_name,
            "total_records": self.total_records,
            "total_checks": len(self.checks),
            "passed": self.passed,
            "failed": self.failed,
            "warnings": self.warnings,
            "critical_failures": self.critical_failures,
            "is_success": self.is_success,
            "duration_seconds": self.duration_seconds,
        }

    def to_json(self) -> str:
        return json.dumps({
            "summary": self.summary(),
            "checks": [c.to_dict() for c in self.checks],
        }, indent=2)

    def raise_on_failure(self) -> None:
        """Raise ValueError if any critical checks failed."""
        if not self.is_success:
            failures = [
                c for c in self.checks
                if c.status == CheckStatus.FAILED and c.severity == Severity.CRITICAL
            ]
            messages = [f"  - {c.check_name}: expected={c.expected}, actual={c.actual}" for c in failures]
            raise ValueError(
                f"Data quality check failed for '{self.table_name}'. "
                f"{len(failures)} critical failure(s):\n" + "\n".join(messages)
            )


# ---------------------------------------------------------------------------
# Quality Runner
# ---------------------------------------------------------------------------
class DataQualityRunner:
    """
    Orchestrates data quality checks on a PySpark DataFrame.

    Add checks via the add_* methods, then call run() to execute
    all checks and produce a QualityReport.
    """

    def __init__(
        self,
        spark: SparkSession,
        df: DataFrame,
        table_name: str = "unknown",
    ):
        self.spark = spark
        self.df = df
        self.table_name = table_name
        self._checks: List[Callable[[], CheckResult]] = []

    # ---- Check Registration Methods ------------------------------------

    def add_null_check(
        self,
        column: str,
        threshold: float = 0.0,
        severity: Severity = Severity.CRITICAL,
    ) -> "DataQualityRunner":
        """
        Check that the null ratio for a column does not exceed threshold.

        Parameters
        ----------
        column : str
        threshold : float
            Maximum allowed null fraction (0.0 = no nulls, 0.05 = 5% allowed).
        severity : Severity
        """
        def _check() -> CheckResult:
            start = time.time()
            total = self.df.count()
            if total == 0:
                return CheckResult(
                    check_name=f"null_check_{column}",
                    check_type="null_check",
                    column=column,
                    severity=severity,
                    status=CheckStatus.WARNING,
                    expected=f"<= {threshold * 100}% nulls",
                    actual="0 rows in DataFrame",
                    duration_ms=(time.time() - start) * 1000,
                )
            null_count = self.df.filter(F.col(column).isNull()).count()
            null_ratio = null_count / total
            passed = null_ratio <= threshold
            return CheckResult(
                check_name=f"null_check_{column}",
                check_type="null_check",
                column=column,
                severity=severity,
                status=CheckStatus.PASSED if passed else CheckStatus.FAILED,
                expected=f"<= {threshold * 100}% nulls",
                actual=f"{null_ratio * 100:.2f}% nulls ({null_count}/{total})",
                duration_ms=(time.time() - start) * 1000,
            )

        self._checks.append(_check)
        return self

    def add_uniqueness_check(
        self,
        column: str,
        severity: Severity = Severity.CRITICAL,
    ) -> "DataQualityRunner":
        """Check that all values in a column are unique (no duplicates)."""
        def _check() -> CheckResult:
            start = time.time()
            total = self.df.count()
            distinct = self.df.select(column).distinct().count()
            duplicates = total - distinct
            passed = duplicates == 0
            return CheckResult(
                check_name=f"uniqueness_check_{column}",
                check_type="uniqueness_check",
                column=column,
                severity=severity,
                status=CheckStatus.PASSED if passed else CheckStatus.FAILED,
                expected="0 duplicates",
                actual=f"{duplicates} duplicates (total={total}, distinct={distinct})",
                duration_ms=(time.time() - start) * 1000,
            )

        self._checks.append(_check)
        return self

    def add_range_check(
        self,
        column: str,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
        severity: Severity = Severity.WARNING,
    ) -> "DataQualityRunner":
        """Check that all non-null values in a column fall within [min_val, max_val]."""
        def _check() -> CheckResult:
            start = time.time()
            non_null = self.df.filter(F.col(column).isNotNull())
            total = non_null.count()
            if total == 0:
                return CheckResult(
                    check_name=f"range_check_{column}",
                    check_type="range_check",
                    column=column,
                    severity=severity,
                    status=CheckStatus.PASSED,
                    expected=f"[{min_val}, {max_val}]",
                    actual="No non-null values",
                    duration_ms=(time.time() - start) * 1000,
                )

            conditions = []
            if min_val is not None:
                conditions.append(F.col(column) < min_val)
            if max_val is not None:
                conditions.append(F.col(column) > max_val)

            if not conditions:
                return CheckResult(
                    check_name=f"range_check_{column}",
                    check_type="range_check",
                    column=column,
                    severity=severity,
                    status=CheckStatus.PASSED,
                    expected="No bounds specified",
                    actual="Skipped",
                    duration_ms=(time.time() - start) * 1000,
                )

            combined = conditions[0]
            for cond in conditions[1:]:
                combined = combined | cond

            violations = non_null.filter(combined).count()
            passed = violations == 0

            stats = non_null.agg(
                F.min(column).alias("min"),
                F.max(column).alias("max"),
            ).collect()[0]

            return CheckResult(
                check_name=f"range_check_{column}",
                check_type="range_check",
                column=column,
                severity=severity,
                status=CheckStatus.PASSED if passed else CheckStatus.FAILED,
                expected=f"[{min_val}, {max_val}]",
                actual=f"{violations} violations (min={stats['min']}, max={stats['max']})",
                duration_ms=(time.time() - start) * 1000,
            )

        self._checks.append(_check)
        return self

    def add_referential_integrity_check(
        self,
        column: str,
        reference_df: DataFrame,
        reference_column: str,
        severity: Severity = Severity.CRITICAL,
    ) -> "DataQualityRunner":
        """Check that all values in column exist in the reference DataFrame."""
        def _check() -> CheckResult:
            start = time.time()
            source_keys = self.df.select(column).distinct()
            ref_keys = reference_df.select(F.col(reference_column).alias(column)).distinct()

            orphans = source_keys.join(ref_keys, on=column, how="left_anti")
            orphan_count = orphans.count()
            total = source_keys.count()
            passed = orphan_count == 0

            return CheckResult(
                check_name=f"ref_integrity_{column}_to_{reference_column}",
                check_type="referential_integrity",
                column=column,
                severity=severity,
                status=CheckStatus.PASSED if passed else CheckStatus.FAILED,
                expected="0 orphaned keys",
                actual=f"{orphan_count} orphaned keys out of {total} distinct values",
                duration_ms=(time.time() - start) * 1000,
            )

        self._checks.append(_check)
        return self

    def add_schema_check(
        self,
        expected_schema: StructType,
        severity: Severity = Severity.CRITICAL,
    ) -> "DataQualityRunner":
        """Check that the DataFrame schema matches the expected schema."""
        def _check() -> CheckResult:
            start = time.time()
            actual_fields = {f.name: f.dataType.simpleString() for f in self.df.schema.fields}
            expected_fields = {f.name: f.dataType.simpleString() for f in expected_schema.fields}

            missing = set(expected_fields.keys()) - set(actual_fields.keys())
            type_mismatches = {
                name: (expected_fields[name], actual_fields.get(name))
                for name in expected_fields
                if name in actual_fields and expected_fields[name] != actual_fields[name]
            }

            passed = len(missing) == 0 and len(type_mismatches) == 0
            details = ""
            if missing:
                details += f"Missing columns: {missing}. "
            if type_mismatches:
                details += f"Type mismatches: {type_mismatches}."

            return CheckResult(
                check_name="schema_compatibility_check",
                check_type="schema_check",
                column=None,
                severity=severity,
                status=CheckStatus.PASSED if passed else CheckStatus.FAILED,
                expected=f"{len(expected_fields)} columns",
                actual=f"{len(actual_fields)} columns",
                details=details,
                duration_ms=(time.time() - start) * 1000,
            )

        self._checks.append(_check)
        return self

    def add_custom_sql_check(
        self,
        check_name: str,
        sql_query: str,
        expected_result: Any,
        comparison: str = "eq",
        severity: Severity = Severity.WARNING,
    ) -> "DataQualityRunner":
        """
        Run a custom SQL check.

        The sql_query should return a single row with a single column named 'result'.
        Comparison operators: eq, gt, lt, gte, lte, between.

        Parameters
        ----------
        check_name : str
        sql_query : str
            SQL returning a single value named 'result'.
        expected_result : Any
            Expected value or tuple for 'between'.
        comparison : str
            Comparison operator.
        severity : Severity
        """
        def _check() -> CheckResult:
            start = time.time()
            try:
                self.df.createOrReplaceTempView("_dq_temp_table")
                result_df = self.spark.sql(sql_query)
                actual = result_df.collect()[0]["result"]

                if comparison == "eq":
                    passed = actual == expected_result
                elif comparison == "gt":
                    passed = actual > expected_result
                elif comparison == "lt":
                    passed = actual < expected_result
                elif comparison == "gte":
                    passed = actual >= expected_result
                elif comparison == "lte":
                    passed = actual <= expected_result
                elif comparison == "between":
                    low, high = expected_result
                    passed = low <= actual <= high
                else:
                    passed = False

                return CheckResult(
                    check_name=check_name,
                    check_type="custom_sql",
                    column=None,
                    severity=severity,
                    status=CheckStatus.PASSED if passed else CheckStatus.FAILED,
                    expected=f"{comparison} {expected_result}",
                    actual=str(actual),
                    duration_ms=(time.time() - start) * 1000,
                )
            except Exception as exc:
                return CheckResult(
                    check_name=check_name,
                    check_type="custom_sql",
                    column=None,
                    severity=severity,
                    status=CheckStatus.ERROR,
                    expected=f"{comparison} {expected_result}",
                    actual=f"ERROR: {exc}",
                    duration_ms=(time.time() - start) * 1000,
                )

        self._checks.append(_check)
        return self

    def add_row_count_check(
        self,
        min_count: int = 1,
        max_count: Optional[int] = None,
        severity: Severity = Severity.CRITICAL,
    ) -> "DataQualityRunner":
        """Check that the row count is within the expected range."""
        def _check() -> CheckResult:
            start = time.time()
            total = self.df.count()
            passed = total >= min_count
            if max_count is not None:
                passed = passed and total <= max_count
            expected_str = f">= {min_count}"
            if max_count is not None:
                expected_str += f" and <= {max_count}"
            return CheckResult(
                check_name="row_count_check",
                check_type="row_count",
                column=None,
                severity=severity,
                status=CheckStatus.PASSED if passed else CheckStatus.FAILED,
                expected=expected_str,
                actual=str(total),
                duration_ms=(time.time() - start) * 1000,
            )

        self._checks.append(_check)
        return self

    def add_accepted_values_check(
        self,
        column: str,
        accepted_values: List[Any],
        severity: Severity = Severity.WARNING,
    ) -> "DataQualityRunner":
        """Check that all non-null values are within the accepted set."""
        def _check() -> CheckResult:
            start = time.time()
            non_null = self.df.filter(F.col(column).isNotNull())
            violations = non_null.filter(~F.col(column).isin(accepted_values)).count()
            total = non_null.count()
            passed = violations == 0
            return CheckResult(
                check_name=f"accepted_values_{column}",
                check_type="accepted_values",
                column=column,
                severity=severity,
                status=CheckStatus.PASSED if passed else CheckStatus.FAILED,
                expected=f"values in {accepted_values}",
                actual=f"{violations} violations out of {total} non-null rows",
                duration_ms=(time.time() - start) * 1000,
            )

        self._checks.append(_check)
        return self

    # ---- Execution ------------------------------------------------------

    def run(self) -> QualityReport:
        """Execute all registered checks and return the aggregated report."""
        report = QualityReport(
            table_name=self.table_name,
            total_records=self.df.count(),
        )

        logger.info(
            "Running %d quality checks on '%s' (%d records).",
            len(self._checks), self.table_name, report.total_records,
        )

        for check_fn in self._checks:
            try:
                result = check_fn()
            except Exception as exc:
                result = CheckResult(
                    check_name="unknown",
                    check_type="error",
                    column=None,
                    severity=Severity.CRITICAL,
                    status=CheckStatus.ERROR,
                    expected="N/A",
                    actual=f"ERROR: {exc}",
                )
            report.checks.append(result)
            logger.info(
                "  [%s] %s: %s (%.0fms)",
                result.status.value.upper(),
                result.check_name,
                result.actual,
                result.duration_ms,
            )

        report.end_time = time.time()

        logger.info(
            "Quality report for '%s': %d passed, %d failed, %d warnings (%.2fs).",
            report.table_name, report.passed, report.failed,
            report.warnings, report.duration_seconds,
        )

        return report
