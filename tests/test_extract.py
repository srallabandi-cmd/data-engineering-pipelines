"""
Unit tests for the API extraction module.
"""

import json
import pytest
from unittest.mock import MagicMock, patch

import sys
sys.path.insert(0, "spark-jobs/etl")


class TestRateLimiter:
    def test_rate_limiter_enforces_interval(self):
        """Rate limiter should enforce the minimum interval between calls."""
        from extract_api_data import RateLimiter
        import time

        limiter = RateLimiter(requests_per_second=100)
        start = time.monotonic()
        for _ in range(5):
            limiter.wait()
        elapsed = time.monotonic() - start
        # 5 calls at 100/s should take at least ~0.04s
        assert elapsed >= 0.03

    def test_rate_limiter_first_call_instant(self):
        """First call should not wait."""
        from extract_api_data import RateLimiter
        import time

        limiter = RateLimiter(requests_per_second=1)
        start = time.monotonic()
        limiter.wait()
        elapsed = time.monotonic() - start
        assert elapsed < 0.1


class TestResponseValidation:
    def test_validates_complete_records(self):
        """Valid records should pass validation."""
        from extract_api_data import validate_response

        data = {
            "data": [
                {
                    "event_id": "e1",
                    "user_id": "u1",
                    "event_type": "click",
                    "event_timestamp": "2024-01-15T10:00:00Z",
                },
                {
                    "event_id": "e2",
                    "user_id": "u2",
                    "event_type": "page_view",
                    "event_timestamp": "2024-01-15T10:01:00Z",
                },
            ]
        }
        valid, dead = validate_response(data)
        assert len(valid) == 2
        assert len(dead) == 0

    def test_rejects_records_missing_required_fields(self):
        """Records missing required fields should go to dead letter queue."""
        from extract_api_data import validate_response

        data = {
            "data": [
                {
                    "event_id": "e1",
                    "user_id": "u1",
                    # missing event_type and event_timestamp
                },
            ]
        }
        valid, dead = validate_response(data)
        assert len(valid) == 0
        assert len(dead) == 1
        assert "_rejection_reason" in dead[0]

    def test_handles_empty_response(self):
        """Empty response should produce no records."""
        from extract_api_data import validate_response

        data = {"data": []}
        valid, dead = validate_response(data)
        assert len(valid) == 0
        assert len(dead) == 0

    def test_handles_results_key(self):
        """Response with 'results' key should work too."""
        from extract_api_data import validate_response

        data = {
            "results": [
                {
                    "event_id": "e1",
                    "user_id": "u1",
                    "event_type": "click",
                    "event_timestamp": "2024-01-15T10:00:00Z",
                },
            ]
        }
        valid, dead = validate_response(data)
        assert len(valid) == 1


class TestRetryWithBackoff:
    @patch("extract_api_data.requests.get")
    def test_retries_on_500(self, mock_get):
        """Should retry on 500 status codes."""
        from extract_api_data import retry_with_backoff

        fail_response = MagicMock()
        fail_response.status_code = 500
        fail_response.headers = {}

        success_response = MagicMock()
        success_response.status_code = 200

        call_count = 0

        def side_effect():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                return fail_response
            return success_response

        result = retry_with_backoff(
            side_effect,
            max_retries=5,
            initial_backoff=0.01,
            max_backoff=0.1,
        )
        assert result.status_code == 200
        assert call_count == 3

    def test_returns_on_first_success(self):
        """Should return immediately on a 200 response."""
        from extract_api_data import retry_with_backoff

        response = MagicMock()
        response.status_code = 200

        result = retry_with_backoff(lambda: response, max_retries=3)
        assert result.status_code == 200


class TestCLI:
    def test_parse_args_required(self):
        """Parser should require --api-url and --output-path."""
        from extract_api_data import parse_args

        args = parse_args([
            "--api-url", "https://api.example.com/events",
            "--output-path", "s3a://bucket/raw/events",
        ])
        assert args.api_url == "https://api.example.com/events"
        assert args.output_path == "s3a://bucket/raw/events"
        assert args.output_format == "delta"

    def test_parse_args_optional(self):
        """Parser should accept optional arguments."""
        from extract_api_data import parse_args

        args = parse_args([
            "--api-url", "https://api.example.com/events",
            "--output-path", "/tmp/output",
            "--format", "parquet",
            "--page-size", "100",
            "--batch-date", "2024-01-15",
        ])
        assert args.output_format == "parquet"
        assert args.page_size == 100
        assert args.batch_date == "2024-01-15"
