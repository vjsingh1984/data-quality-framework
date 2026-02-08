# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for DQResult dataclass."""
from dq.result_models import DQResult


class TestDQResult:
    """Tests for DQResult creation, conversion, and round-tripping."""

    def test_create_minimal(self):
        result = DQResult(check="test_check", success=True)
        assert result.check == "test_check"
        assert result.success is True
        assert result.details == {}
        assert result.engine == ""
        assert result.rule_name == ""
        assert result.level == "Error"
        assert result.ts is None
        assert result.job_id is None

    def test_create_full(self):
        result = DQResult(
            check="completeness",
            success=False,
            details={"column": "age", "ratio": 0.95},
            engine="deequ",
            rule_name="quality_rule_1",
            level="Warning",
            ts=1700000000000,
            job_id="app-123",
        )
        assert result.check == "completeness"
        assert result.success is False
        assert result.details == {"column": "age", "ratio": 0.95}
        assert result.engine == "deequ"
        assert result.level == "Warning"
        assert result.ts == 1700000000000
        assert result.job_id == "app-123"

    def test_to_dict(self):
        result = DQResult(
            check="test",
            success=True,
            details={"key": "val"},
            engine="custom",
            rule_name="rule1",
            level="Error",
            ts=123456,
            job_id="app-1",
        )
        d = result.to_dict()
        assert d == {
            "check": "test",
            "success": True,
            "details": {"key": "val"},
            "ts": 123456,
            "jobid": "app-1",
        }
        # engine, rule_name, level are NOT in legacy dict
        assert "engine" not in d
        assert "rule_name" not in d
        assert "level" not in d

    def test_to_dict_none_ts_jobid(self):
        result = DQResult(check="x", success=False)
        d = result.to_dict()
        assert d["ts"] is None
        assert d["jobid"] is None

    def test_from_legacy_dict_minimal(self):
        d = {"check": "my_check", "success": True}
        result = DQResult.from_legacy_dict(d)
        assert result.check == "my_check"
        assert result.success is True
        assert result.details == {}
        assert result.engine == ""
        assert result.ts is None

    def test_from_legacy_dict_full(self):
        d = {
            "check": "hasSize",
            "success": False,
            "details": {"expected": 100, "actual": 50},
            "engine": "deequ",
            "rule_name": "size_rule",
            "level": "Warning",
            "ts": 999999,
            "jobid": "spark-abc",
        }
        result = DQResult.from_legacy_dict(d)
        assert result.check == "hasSize"
        assert result.success is False
        assert result.details["expected"] == 100
        assert result.engine == "deequ"
        assert result.ts == 999999
        assert result.job_id == "spark-abc"

    def test_round_trip(self):
        original = DQResult(
            check="round_trip",
            success=True,
            details={"nested": {"a": 1}},
            ts=111,
            job_id="j1",
        )
        d = original.to_dict()
        restored = DQResult.from_legacy_dict(d)
        assert restored.check == original.check
        assert restored.success == original.success
        assert restored.details == original.details
        assert restored.ts == original.ts
        assert restored.job_id == original.job_id

    def test_from_legacy_dict_empty(self):
        result = DQResult.from_legacy_dict({})
        assert result.check == ""
        assert result.success is False

    def test_equality(self):
        r1 = DQResult(check="a", success=True, ts=1)
        r2 = DQResult(check="a", success=True, ts=1)
        assert r1 == r2

    def test_inequality(self):
        r1 = DQResult(check="a", success=True)
        r2 = DQResult(check="a", success=False)
        assert r1 != r2
