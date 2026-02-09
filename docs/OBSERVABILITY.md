# Observability and Multi-Engine Features

This document describes the observability and multi-engine execution features added to the Data Quality Framework.

## Observability Integration

The framework now supports exporting metrics to external monitoring systems for production observability.

### Supported Exporters

1. **Prometheus** - Expose metrics on an HTTP endpoint for Prometheus scraping
2. **OpenTelemetry** - Send metrics to OTLP-compatible backends
3. **Datadog** - Send metrics to Datadog via DogStatsD or API
4. **Logging** - Fallback exporter that writes structured logs

### Configuration

Enable observability in your HOCON configuration:

```hocon
dqframework {
  observability {
    prometheus_enabled = true
    prometheus_port = 9090

    opentelemetry_enabled = true
    opentelemetry_endpoint = "http://localhost:4318"

    datadog_enabled = false  # Requires datadog package
  }

  dqrules = [...]
}
```

### Metrics Exported

- `dq_check_result` - Pass/fail result for each check (gauge)
- `dq_checks_total` - Total number of checks executed (counter)
- `dq_check_execution_time_ms` - Execution time per check (gauge)
- Performance metrics - Engine execution times

All metrics include labels for `engine`, `dataset`, `constraint`, and `check`.

## Multi-Engine Execution

Execute multiple data quality engines with different strategies:

### Execution Strategies

1. **Sequential** - Execute engines one at a time (default, deterministic)
2. **Parallel** - Execute engines concurrently using threads
3. **Batched** - Group by DataFrame and execute in batches

### Usage

```python
from dq.engine.multi_engine import (
    MultiEngineOrchestrator,
    ExecutionStrategy,
)

# Create orchestrator with parallel execution
orchestrator = MultiEngineOrchestrator(
    strategy=ExecutionStrategy.PARALLEL,
    max_workers=4,
)

# Add engines and dataframes
orchestrator.add_engine(deequ_engine, ["df1", "df2"])
orchestrator.add_engine(custom_engine, ["df1"])
orchestrator.add_dataframe("df1", spark_df1)
orchestrator.add_dataframe("df2", spark_df2)

# Execute all engines
result = orchestrator.run()

# Access results
print(f"Executed: {result.total_engines} engines")
print(f"Successful: {result.successful_engines}")
print(f"Failed: {result.failed_engines}")

# Get metrics
all_metrics = result.get_metrics()

# Filter by engine
deequ_results = result.get_results_by_engine("deequ")

# Filter by DataFrame
df1_results = result.get_results_by_dataframe("df1")

# Get failed results
failed_results = result.get_failed_results()
```

## Performance Monitoring

Track engine execution times and resource usage automatically:

```python
from dq.observability import PerformanceMonitor, PrometheusExporter

# Create exporter
exporter = PrometheusExporter(port=9090)
exporter.start()

# Create performance monitor
monitor = PerformanceMonitor(exporter=exporter)

# Monitor engine execution
with monitor.monitor_engine("deequ", "my_table"):
    result = engine.apply(dataframe)

# Get performance summary
summary = monitor.get_summary()
print(f"Total time: {summary['total_execution_time_ms']} ms")
```

## Profiling Engine

Generate data profiles to understand data characteristics:

```hocon
dqframework {
  dqrules = [
    {
      name = "Profile Orders"
      engine = "profiler"
      dataframes = ["orders_table"]

      profile_type = "comprehensive"
      include_correlation = false
      max_unique_values = 100

      # Export profile report
      export_path = "/tmp/profiles/orders.html"
      export_format = "html"  # json, html, or markdown
    }
  ]
}
```

### Profile Types

- **basic**: Row/column counts, null analysis, basic statistics
- **comprehensive**: Everything in basic + unique values, percentiles, patterns
- **advanced**: Everything in comprehensive + correlations, distributions, outliers

### Export Formats

- **JSON**: Structured data for programmatic access
- **HTML**: Styled report with visualizations
- **Markdown**: Documentation format

## Example Configurations

### Complete Example

See `examples/complete_validation.conf` for a full configuration example.

### Profiling Only

```hocon
dqframework {
  catalog_type = "spark"

  dataframes {
    my_table = "catalog.schema.my_table"
  }

  dqrules = [
    {
      name = "Data Profiling"
      engine = "profiler"
      dataframes = ["my_table"]
      profile_type = "comprehensive"
      export_path = "/tmp/profile.html"
      export_format = "html"
    }
  ]
}
```

### Multi-Engine with Observability

```hocon
dqframework {
  observability {
    prometheus_enabled = true
    prometheus_port = 9090
  }

  dataframes {
    users = "db.users"
    orders = "db.orders"
  }

  dqrules = [
    {
      name = "Deequ Validation"
      engine = "deequ"
      dataframes = ["users", "orders"]
      checks = [...]
    }
    {
      name = "Custom Validation"
      engine = "constraint"
      dataframes = ["users", "orders"]
      checks = [...]
    }
  ]
}
```

## Performance Considerations

- **Sequential execution** is safest for resource-constrained environments
- **Parallel execution** can significantly reduce total time for I/O-bound engines
- **Batched execution** is optimal when multiple engines operate on the same DataFrame
- **Observability overhead** is minimal (<5ms per check for logging exporter)

## Installation

### Observability Dependencies

```bash
# For Prometheus support
pip install prometheus_client

# For OpenTelemetry support
pip install opentelemetry-sdk opentelemetry-api

# For Datadog support
pip install datadog
```

These packages are optional - the framework will gracefully fall back to logging if they're not installed.
