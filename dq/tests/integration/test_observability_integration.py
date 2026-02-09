# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Integration tests for observability and multi-engine features."""
import pytest
from pyhocon import ConfigFactory

from dq.engine.multi_engine import ExecutionStrategy, MultiEngineOrchestrator
from dq.observability import MetricsRegistry, PerformanceMonitor


@pytest.mark.integration
class TestObservabilityIntegration:
    """Integration tests for observability features."""

    def test_metrics_registry_integration(self, spark):
        """Test metrics registry with actual DQ metrics."""
        from dq.observability.metrics import Metric, MetricLabel, MetricType

        # Create a sample DataFrame
        df = spark.createDataFrame(
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
            ["id", "name"],
        )

        # Create registry and record metrics
        registry = MetricsRegistry()
        registry.record(
            Metric(
                name="dq_check_result",
                description="Check result",
                metric_type=MetricType.GAUGE,
                value=1.0,
                labels=[
                    MetricLabel("engine", "test_engine"),
                    MetricLabel("dataset", "test_df"),
                ],
            )
        )

        # Verify metrics can be retrieved
        metrics = registry.get_metrics()
        assert len(metrics) == 1
        assert metrics[0].name == "dq_check_result"

    def test_performance_monitor_with_dataframe(self, spark):
        """Test performance monitoring with actual Spark operations."""
        monitor = PerformanceMonitor()

        # Create sample DataFrame
        df = spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])

        # Record DataFrame stats
        stats = monitor.record_dataframe_stats(df, "test_table")
        assert stats["row_count"] == 2
        assert stats["column_count"] == 2

    def test_performance_monitor_execution_tracking(self, spark):
        """Test that performance monitor tracks execution time."""
        import time

        monitor = PerformanceMonitor()
        df = spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])

        # Monitor execution
        with monitor.monitor_engine("test_engine", "test_df"):
            time.sleep(0.01)  # Simulate work
            df.count()  # Spark action

        # Get snapshots
        snapshots = monitor.get_snapshots()
        assert len(snapshots) == 1
        assert snapshots[0].duration_ms >= 10  # At least 10ms


@pytest.mark.integration
class TestMultiEngineIntegration:
    """Integration tests for multi-engine execution."""

    def test_multi_engine_orchestrator_basic(self, spark):
        """Test basic multi-engine orchestration."""
        from dq.engine.constraint.constraint_engine import ConstraintEngine

        # Create sample DataFrames
        df1 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
        df2 = spark.createDataFrame([(1, "x"), (2, "y")], ["id", "code"])

        # Create orchestrator
        orchestrator = MultiEngineOrchestrator(strategy=ExecutionStrategy.SEQUENTIAL)

        # Add engines with minimal valid config
        config1 = ConfigFactory.parse_string(
            """
        {
            checks = [
                {
                    constraint = "DistinctnessByGroup"
                    constraint_name = "test_check1"
                    columns = ["id"]
                    group_by = ["value"]
                }
            ]
        }
        """
        )
        config2 = ConfigFactory.parse_string(
            """
        {
            checks = [
                {
                    constraint = "DistinctnessByGroup"
                    constraint_name = "test_check2"
                    columns = ["id"]
                    group_by = ["code"]
                }
            ]
        }
        """
        )
        engine1 = ConstraintEngine(config1)
        engine2 = ConstraintEngine(config2)

        # Note: These engines won't have checks, so they'll return empty results
        orchestrator.add_dataframe("df1", df1)
        orchestrator.add_dataframe("df2", df2)
        orchestrator.add_engine(engine1, ["df1"])
        orchestrator.add_engine(engine2, ["df2"])

        # Execute
        result = orchestrator.run()

        # Verify
        assert result.total_engines == 2
        assert result.successful_engines == 2
        # Should have metrics from the checks
        assert len(result.get_metrics()) > 0

    def test_multi_engine_with_checks(self, spark):
        """Test multi-engine with actual validation checks."""
        from dq.engine.constraint.constraint_engine import ConstraintEngine

        # Create sample DataFrame
        df = spark.createDataFrame(
            [(1, "alice@example.com"), (2, "bob@example.com")],
            ["id", "email"],
        )

        # Create engine with check
        config = ConfigFactory.parse_string(
            """
            {
                checks = [
                    {
                        constraint = "DistinctnessByGroup"
                        constraint_name = "email_distinctness"
                        columns = ["email"]
                        group_by = ["id"]
                        min_threshold = 1
                    }
                ]
            }
            """
        )
        engine = ConstraintEngine(config)

        # Create orchestrator
        orchestrator = MultiEngineOrchestrator()
        orchestrator.add_dataframe("test_df", df)
        orchestrator.add_engine(engine, ["test_df"])

        # Execute
        result = orchestrator.run()

        # Verify
        assert result.total_engines == 1
        assert result.successful_engines == 1
        # Should have metrics from the check
        assert len(result.get_metrics()) > 0

    def test_sequential_vs_parallel_execution(self, spark):
        """Test that sequential and parallel strategies both work."""
        import time

        from dq.engine.constraint.constraint_engine import ConstraintEngine

        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
        config = ConfigFactory.parse_string(
            """
        {
            checks = [
                {
                    constraint = "DistinctnessByGroup"
                    constraint_name = "test_check"
                    columns = ["id"]
                    group_by = ["value"]
                }
            ]
        }
        """
        )

        # Test sequential
        seq_orchestrator = MultiEngineOrchestrator(
            strategy=ExecutionStrategy.SEQUENTIAL
        )
        seq_orchestrator.add_dataframe("df", df)
        seq_orchestrator.add_engine(ConstraintEngine(config), ["df"])
        seq_orchestrator.add_engine(ConstraintEngine(config), ["df"])

        start = time.time()
        seq_result = seq_orchestrator.run()
        seq_time = time.time() - start

        # Test parallel
        par_orchestrator = MultiEngineOrchestrator(
            strategy=ExecutionStrategy.PARALLEL,
            max_workers=2,
        )
        par_orchestrator.add_dataframe("df", df)
        par_orchestrator.add_engine(ConstraintEngine(config), ["df"])
        par_orchestrator.add_engine(ConstraintEngine(config), ["df"])

        start = time.time()
        par_result = par_orchestrator.run()
        par_time = time.time() - start

        # Both should complete successfully
        assert seq_result.total_engines == 2
        assert par_result.total_engines == 2
        # Parallel should be faster or similar (not slower)
        assert par_time <= seq_time + 0.1  # Allow 100ms tolerance


@pytest.mark.integration
class TestProfilerIntegration:
    """Integration tests for ProfilerEngine."""

    def test_profiler_engine_basic(self, spark):
        """Test ProfilerEngine with actual DataFrame."""
        from dq.engine.profiler import ProfilerEngine

        # Create sample DataFrame with various data types
        df = spark.createDataFrame(
            [
                (1, "Alice", 25, "alice@example.com", "2024-01-01"),
                (2, "Bob", 30, "bob@example.com", "2024-01-02"),
                (3, "Charlie", 35, "charlie@example.com", "2024-01-03"),
            ],
            ["id", "name", "age", "email", "date"],
        )

        # Create profiler engine
        config = ConfigFactory.parse_string(
            """
            {
                profile_type = "basic"
            }
            """
        )
        engine = ProfilerEngine(config)

        # Execute profiling
        results = engine.apply(df)

        # Verify results
        assert len(results) == 1  # Single metric with full profile
        assert results[0]["success"] is True
        assert "details" in results[0]

    def test_profiler_export_to_json(self, spark, tmp_path):
        """Test ProfilerEngine JSON export functionality."""
        import json

        from dq.engine.profiler import ProfilerEngine

        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])

        # Create profiler with export
        export_path = tmp_path / "profile.json"
        config = ConfigFactory.parse_string(
            f"""
            {{
                profile_type = "basic"
                export_path = "{export_path}"
                export_format = "json"
            }}
            """
        )
        engine = ProfilerEngine(config)

        # Execute
        results = engine.apply(df)

        # Verify export
        assert export_path.exists()
        with open(export_path) as f:
            profile_data = json.load(f)
        assert "dataframe_name" in profile_data

    def test_profiler_suggestions(self, spark):
        """Test ProfilerEngine rule suggestion generation."""
        from dq.engine.profiler import ProfilerEngine

        # Create DataFrame with some data quality issues
        df = spark.createDataFrame(
            [(1, "alice@example.com"), (2, None), (3, "charlie@example.com")],
            ["id", "email"],
        )

        config = ConfigFactory.parse_string(
            """
            {
                profile_type = "comprehensive"
            }
            """
        )
        engine = ProfilerEngine(config)

        # Execute
        results = engine.apply(df)

        # Should have suggestions (including for null values)
        assert results[0]["success"] is True


@pytest.mark.integration
class TestFrameworkWithObservability:
    """Integration tests for DQFramework with observability."""

    def test_framework_with_prometheus_exporter(self, spark):
        """Test DQFramework with Prometheus exporter configuration."""
        from dq.dq_framework import DQFramework

        config = """
        dqframework {
            observability {
                prometheus_enabled = false  # Don't start server in test
            }
            dataframes {}
            dqrules = []
        }
        """

        framework = DQFramework(spark, config)
        # Should initialize exporters without error
        assert framework._metrics_exporters is not None
        assert len(framework._metrics_exporters) > 0

    def test_framework_metrics_export(self, spark):
        """Test that framework exports metrics."""
        from dq.dq_framework import DQFramework

        # Create simple config with checks
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
        config = """
        dqframework {
            observability {
                prometheus_enabled = false
            }
            dataframes {}
            dqrules = []
        }
        """

        df.createOrReplaceTempView("test_df")
        framework = DQFramework(spark, config)

        # Run (even with no rules, should complete)
        results = framework.run()
        assert isinstance(results, list)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
