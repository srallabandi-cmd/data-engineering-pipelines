"""
Custom Data Quality Framework
==============================

Provides reusable data quality checks for PySpark DataFrames with configurable
severity levels, automatic report generation, and extensible validation rules.

Checks included:
- Column null percentage
- Column uniqueness
- Referential integrity
- Numeric range validation
- Regex pattern validation
- Statistical anomaly detection (z-score)

Usage:
    from data_quality import DataQualityRunner, Check, Severity

    runner = DataQualityRunner(spark)
    runner.add_check(Check.null_check("user_id", max_null_pct=0.0, severity=Severity.FAIL))
    runner.add_check(Check.unique_check("event_id", severity=Severity.FAIL))
    runner.add_check(Check.range_check("amount", min_val=0, max_val=10000))
    report = runner.run(df)
    report.raise_on_failure()
"""

import json
import logging
import math
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("data_quality")


# ---------------------------------------------------------------------------
# Severity
# ---------------------------------------------------------------------------
class Severity(str, Enum):
    WARN = "warn"
    FAIL = "fail"


# ---------------------------------------------------------------------------
# Check result
# ---------------------------------------------------------------------------
@dataclass
class CheckResult:
    check_name: str
    column: Optional[str]
    passed: bool
    severity: Severity
    metric_value: Any = None
    threshold: Any = None
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Quality report
# ---------------------------------------------------------------------------
@dataclass
class QualityReport:
    results: List[CheckResult] = field(default_factory=list)
    generated_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    @property
    def passed(self) -> bool:
        return all(r.passed or r.severity == Severity.WARN for r in self.results)

    @property
    def failures(self) -> List[CheckResult]:
        return [r for r in self.results if not r.passed and r.severity == Severity.FAIL]

    @property
    def warnings(self) -> List[CheckResult]:
        return [r for r in self.results if not r.passed and r.severity == Severity.WARN]

    def raise_on_failure(self) -> None:
        """Raise ``ValueError`` if any FAIL-severity check did not pass."""
        if self.failures:
            msgs = [f"  - {f.check_name} ({f.column}): {f.message}" for f in self.failures]
            raise ValueError(
                f"Data quality gate FAILED with {len(self.failures)} error(s):\n"
                + "\n".join(msgs)
            )

    def to_dict(self) -> Dict:
        return {
            "generated_at": self.generated_at,
            "overall_passed": self.passed,
            "total_checks": len(self.results),
            "failures": len(self.failures),
            "warnings": len(self.warnings),
            "results": [
                {
                    "check_name": r.check_name,
                    "column": r.column,
                    "passed": r.passed,
                    "severity": r.severity.value,
                    "metric_value": r.metric_value,
                    "threshold": r.threshold,
                    "message": r.message,
                }
                for r in self.results
            ],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, default=str)

    def log_summary(self) -> None:
        logger.info("=== Data Quality Report ===")
        logger.info(
            "Total: %d | Passed: %d | Warnings: %d | Failures: %d",
            len(self.results),
            sum(1 for r in self.results if r.passed),
            len(self.warnings),
            len(self.failures),
        )
        for r in self.results:
            status = "PASS" if r.passed else r.severity.value.upper()
            logger.info("  [%s] %s (%s): %s", status, r.check_name, r.column, r.message)


# ---------------------------------------------------------------------------
# Check definitions
# ---------------------------------------------------------------------------
@dataclass
class Check:
    name: str
    column: Optional[str]
    severity: Severity
    validator: Callable[[DataFrame], CheckResult]

    # -- Factory methods ---------------------------------------------------

    @staticmethod
    def null_check(
        column: str,
        max_null_pct: float = 0.0,
        severity: Severity = Severity.FAIL,
    ) -> "Check":
        """Fail if the percentage of null values exceeds *max_null_pct* (0-1)."""

        def _validate(df: DataFrame) -> CheckResult:
            total = df.count()
            if total == 0:
                return CheckResult(
                    check_name="null_check",
                    column=column,
                    passed=False,
                    severity=severity,
                    message="DataFrame is empty",
                )
            null_count = df.filter(F.col(column).isNull()).count()
            null_pct = null_count / total
            passed = null_pct <= max_null_pct
            return CheckResult(
                check_name="null_check",
                column=column,
                passed=passed,
                severity=severity,
                metric_value=round(null_pct, 6),
                threshold=max_null_pct,
                message=f"null_pct={null_pct:.4%} (threshold={max_null_pct:.4%})",
            )

        return Check(
            name="null_check", column=column, severity=severity, validator=_validate
        )

    @staticmethod
    def unique_check(
        column: str,
        severity: Severity = Severity.FAIL,
    ) -> "Check":
        """Fail if the column contains duplicate values."""

        def _validate(df: DataFrame) -> CheckResult:
            total = df.count()
            distinct = df.select(column).distinct().count()
            dup_count = total - distinct
            passed = dup_count == 0
            return CheckResult(
                check_name="unique_check",
                column=column,
                passed=passed,
                severity=severity,
                metric_value=dup_count,
                threshold=0,
                message=f"duplicates={dup_count} out of {total}",
            )

        return Check(
            name="unique_check", column=column, severity=severity, validator=_validate
        )

    @staticmethod
    def referential_integrity_check(
        column: str,
        reference_df: DataFrame,
        reference_column: str,
        max_orphan_pct: float = 0.0,
        severity: Severity = Severity.FAIL,
    ) -> "Check":
        """Fail if more than *max_orphan_pct* of values in *column* are not
        present in the reference DataFrame's *reference_column*.
        """

        def _validate(df: DataFrame) -> CheckResult:
            total = df.count()
            if total == 0:
                return CheckResult(
                    check_name="referential_integrity",
                    column=column,
                    passed=False,
                    severity=severity,
                    message="DataFrame is empty",
                )
            orphan_df = df.join(
                reference_df.select(F.col(reference_column).alias(column)),
                on=column,
                how="left_anti",
            )
            orphan_count = orphan_df.count()
            orphan_pct = orphan_count / total
            passed = orphan_pct <= max_orphan_pct
            return CheckResult(
                check_name="referential_integrity",
                column=column,
                passed=passed,
                severity=severity,
                metric_value=round(orphan_pct, 6),
                threshold=max_orphan_pct,
                message=f"orphan_pct={orphan_pct:.4%} ({orphan_count}/{total})",
            )

        return Check(
            name="referential_integrity",
            column=column,
            severity=severity,
            validator=_validate,
        )

    @staticmethod
    def range_check(
        column: str,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
        severity: Severity = Severity.FAIL,
    ) -> "Check":
        """Fail if any non-null value in *column* falls outside [min_val, max_val]."""

        def _validate(df: DataFrame) -> CheckResult:
            conditions = []
            if min_val is not None:
                conditions.append(F.col(column) < min_val)
            if max_val is not None:
                conditions.append(F.col(column) > max_val)

            if not conditions:
                return CheckResult(
                    check_name="range_check",
                    column=column,
                    passed=True,
                    severity=severity,
                    message="No bounds specified",
                )

            combined = conditions[0]
            for c in conditions[1:]:
                combined = combined | c

            violations = df.filter(F.col(column).isNotNull()).filter(combined).count()
            passed = violations == 0
            return CheckResult(
                check_name="range_check",
                column=column,
                passed=passed,
                severity=severity,
                metric_value=violations,
                threshold=f"[{min_val}, {max_val}]",
                message=f"out_of_range_count={violations}",
            )

        return Check(
            name="range_check", column=column, severity=severity, validator=_validate
        )

    @staticmethod
    def regex_check(
        column: str,
        pattern: str,
        max_violation_pct: float = 0.0,
        severity: Severity = Severity.FAIL,
    ) -> "Check":
        """Fail if more than *max_violation_pct* of non-null values do not match *pattern*."""

        def _validate(df: DataFrame) -> CheckResult:
            non_null = df.filter(F.col(column).isNotNull())
            total = non_null.count()
            if total == 0:
                return CheckResult(
                    check_name="regex_check",
                    column=column,
                    passed=True,
                    severity=severity,
                    message="No non-null values to check",
                )

            violations = non_null.filter(~F.col(column).rlike(pattern)).count()
            violation_pct = violations / total
            passed = violation_pct <= max_violation_pct
            return CheckResult(
                check_name="regex_check",
                column=column,
                passed=passed,
                severity=severity,
                metric_value=round(violation_pct, 6),
                threshold=max_violation_pct,
                message=f"violation_pct={violation_pct:.4%} pattern='{pattern}'",
            )

        return Check(
            name="regex_check", column=column, severity=severity, validator=_validate
        )

    @staticmethod
    def zscore_anomaly_check(
        column: str,
        max_zscore: float = 3.0,
        max_anomaly_pct: float = 0.01,
        severity: Severity = Severity.WARN,
    ) -> "Check":
        """Warn/fail if more than *max_anomaly_pct* of values have a z-score
        exceeding *max_zscore*.
        """

        def _validate(df: DataFrame) -> CheckResult:
            stats = df.select(
                F.mean(column).alias("mean"),
                F.stddev(column).alias("stddev"),
                F.count(column).alias("cnt"),
            ).collect()[0]

            mean_val = stats["mean"]
            stddev_val = stats["stddev"]
            count_val = stats["cnt"]

            if count_val == 0 or stddev_val is None or stddev_val == 0:
                return CheckResult(
                    check_name="zscore_anomaly",
                    column=column,
                    passed=True,
                    severity=severity,
                    message="Insufficient data or zero variance",
                )

            anomaly_count = (
                df.filter(F.col(column).isNotNull())
                .filter(
                    F.abs((F.col(column) - F.lit(mean_val)) / F.lit(stddev_val))
                    > max_zscore
                )
                .count()
            )

            anomaly_pct = anomaly_count / count_val
            passed = anomaly_pct <= max_anomaly_pct
            return CheckResult(
                check_name="zscore_anomaly",
                column=column,
                passed=passed,
                severity=severity,
                metric_value=round(anomaly_pct, 6),
                threshold=max_anomaly_pct,
                message=(
                    f"anomaly_pct={anomaly_pct:.4%} "
                    f"(mean={mean_val:.2f} std={stddev_val:.2f} z_thresh={max_zscore})"
                ),
                details={
                    "mean": mean_val,
                    "stddev": stddev_val,
                    "anomaly_count": anomaly_count,
                },
            )

        return Check(
            name="zscore_anomaly",
            column=column,
            severity=severity,
            validator=_validate,
        )


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------
class DataQualityRunner:
    """Runs a collection of ``Check`` instances against a DataFrame and
    produces a ``QualityReport``.
    """

    def __init__(self, spark: Optional[SparkSession] = None) -> None:
        self._spark = spark
        self._checks: List[Check] = []

    def add_check(self, check: Check) -> "DataQualityRunner":
        self._checks.append(check)
        return self

    def add_checks(self, checks: List[Check]) -> "DataQualityRunner":
        self._checks.extend(checks)
        return self

    def run(self, df: DataFrame) -> QualityReport:
        """Execute all registered checks and return a QualityReport."""
        logger.info("Running %d quality checks …", len(self._checks))
        report = QualityReport()

        for check in self._checks:
            try:
                result = check.validator(df)
                report.results.append(result)
            except Exception as exc:
                report.results.append(
                    CheckResult(
                        check_name=check.name,
                        column=check.column,
                        passed=False,
                        severity=check.severity,
                        message=f"Check raised exception: {exc}",
                    )
                )

        report.log_summary()
        return report
