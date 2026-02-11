# Ready for Production

This guide covers the essentials for running Oban in production: logging, job maintenance, and
error monitoring.

## Using the CLI (Recommended)

If you're using `oban start` to run your workers, **you're already production-ready.**

The CLI automatically enables:

- ✅ **Logging**: Structured logs for all job and system events
- ✅ **Job Pruning**: Removes completed jobs after 1 day (configurable via `oban.toml`)
- ✅ **Orphan Rescue**: Recovers jobs interrupted by crashes or deployments

Simply start Oban with your configuration:

```bash
oban start
```

### Configuring Retention

You can adjust how long completed jobs are kept by configuring the pruner in `oban.toml`:

```toml
[pruner]
max_age = 3600  # Keep jobs for 1 hour instead of 1 day
```

That's it! The CLI handles everything else automatically.

## Running Embedded

If you're running Oban embedded in your application instead of using the CLI, you'll need to
configure a few things manually.

### Enable Logging

Attach the telemetry logger during application startup:

```python
from oban.telemetry import logger as telemetry_logger

telemetry_logger.attach()
```

The logger emits JSON-encoded structured logs at the `INFO` level by default. You can customize
the log level:

```python
telemetry_logger.attach(level=logging.DEBUG)
```

### Configure Job Maintenance

Pruning and orphan rescue are enabled by default, but you can customize them:

```python
from oban import Oban

pool = await Oban.create_pool()

oban = Oban(
    pool=pool,
    queues={"default": 10},
    pruner={"max_age": 3_600},  # Keep jobs for 1 hour
    lifeline={"interval": 30},  # Check for orphans every 30 seconds
)
```

See the [Job Maintenance](job_maintenance.md) guide for more details on job retention.

### Enable Metrics for Web Dashboard

To use the Oban Web dashboard, enable metrics broadcasting:

```python
oban = Oban(pool=pool, queues={"default": 10}, metrics=True)
```

See the [Web Dashboard](web_dashboard.md) guide for setup instructions.

## Ship It!

Whether you're using the CLI or embedded mode, you now have:

- ✅ **Structured logging** for debugging and monitoring
- ✅ **Automatic job pruning** to prevent unbounded table growth
- ✅ **Orphan recovery** for interrupted jobs

The CLI handles all of this automatically, making it the recommended approach for most production
deployments.
