<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/oban-bg/oban/main/assets/oban-logotype-dark.png">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/oban-bg/oban/main/assets/oban-logotype-light.png">
    <img alt="Oban logo" src="https://raw.githubusercontent.com/oban-bg/oban/main/assets/oban-logotype-light.png" width="320">
  </picture>
</p>

<p align="center">
  Oban is a robust job orchestration framework for Python, backed by PostgreSQL.
  Reliable, <br /> observable, and loaded with <a href="#features">enterprise grade features</a>.
</p>

<p align="center">
  <a href="https://pypi.org/project/oban/">
    <img alt="PyPI Version" src="https://img.shields.io/pypi/v/oban.svg">
  </a>

  <a href="https://github.com/oban-bg/oban-py/actions">
    <img alt="CI Status" src="https://github.com/oban-bg/oban-py/workflows/ci/badge.svg">
  </a>

  <a href="https://opensource.org/licenses/Apache-2.0">
    <img alt="Apache 2 License" src="https://img.shields.io/pypi/l/oban">
  </a>
</p>

## Table of Contents

- [Features](#features)
- [Oban Pro](#-oban-pro)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick Getting Started](#quick-getting-started)
- [Also Available](#also-available)
- [Community](#community)
- [Contributing](#contributing)

---

> [!NOTE]
>
> This README is for the unreleased main branch, please reference the [official documentation]
> [docs] for the latest stable release.

[docs]: https://oban.pro/docs/oban-py

---

<!-- INDEX START -->

## Features

Oban's primary goals are **reliability**, **consistency** and **observability**.

Oban is a powerful and flexible library that can handle a wide range of background job use cases,
and it is well-suited for systems of any size. It provides a simple and consistent API for
scheduling and performing jobs, and it is built to be fault-tolerant and easy to monitor.

Oban is fundamentally different from other background job processing tools because _it retains job
data for historic metrics and inspection_. You can leave your application running indefinitely
without worrying about jobs being lost or orphaned due to crashes.

### Advantages Over Other Tools

- **Async Native** — Built entirely on asyncio with async/await throughout. Integrates naturally
  with async web frameworks.

- **Fewer Dependencies** — If you are running a web app there is a _very good_ chance that you're
  running on top of a SQL database. Running your job queue within a SQL database minimizes system
  dependencies and simplifies data backups.

- **Transactional Control** — Enqueue a job along with other database changes, ensuring that
  everything is committed or rolled back atomically.

- **Database Backups** — Jobs are stored inside of your primary database, which means they are
  backed up together with the data that they relate to.

### Advanced Features

- **Isolated Queues** — Jobs are stored in a single table but are executed in distinct queues.
  Each queue runs in isolation, with its own concurrency limits, ensuring that a job in a single
  slow queue can't back up other faster queues.

- **Queue Control** — Queues can be started, stopped, paused, resumed and scaled independently at
  runtime locally or across _all_ running nodes.

- **Resilient Queues** — Failing queries won't crash the entire process, instead a backoff
  mechanism will safely retry them again in the future.

- **Job Canceling** — Jobs can be canceled regardless of which node they are running on. For
  executing jobs, workers can check for cancellation at safe points and stop gracefully.

- **Triggered Execution** — Insert triggers ensure that jobs are dispatched on all connected nodes
  as soon as they are inserted into the database.

- **Unique Jobs** — Duplicate work can be avoided through unique job controls. Uniqueness can be
  enforced at the argument, queue, worker and even sub-argument level for any period of time.

- **Scheduled Jobs** — Jobs can be scheduled at any time in the future, down to the second.

- **Periodic (CRON) Jobs** — Automatically enqueue jobs on a cron-like schedule. Duplicate jobs
  are never enqueued, no matter how many nodes you're running.

- **Job Priority** — Prioritize jobs within a queue to run ahead of others with ten levels of
  granularity.

- **Historic Metrics** — After a job is processed the row isn't deleted. Instead, the job is
  retained in the database to provide metrics. This allows users to inspect historic jobs and to
  see aggregate data at the job, queue or argument level.

- **Node Metrics** — Every queue records metrics to the database during runtime. These are used to
  monitor queue health across nodes and may be used for analytics.

- **Graceful Shutdown** — Queue shutdown is delayed so that slow jobs can finish executing before
  shutdown. When shutdown starts queues are paused and stop executing new jobs. Any jobs left
  running after the shutdown grace period may be rescued later.

- **Telemetry Integration** — Job life-cycle events are emitted via Telemetry integration.
  This enables simple logging, error reporting and health checkups without plug-ins.

## Oban Pro

An official set of extensions that expand what Oban is capable of are coming soon...

## Requirements

Oban requires:

* Python 3.12+
* PostgreSQL 14.0+

## Installation

See the [installation guide][install] for details on installing and configuring Oban in your
application.

[install]: https://oban.pro/docs/oban-py

## Quick Getting Started

1. Configure Oban with a connection pool and queues:

   ```python
   # app/oban.py
   from oban import Oban

   pool = await Oban.create_pool()
   oban = Oban(pool=pool, queues={"mailers": 20})
   ```

2. Define a worker to process jobs in the `mailers` queue:

   ```python
   # app/workers.py
   from oban import worker

   @worker(queue="mailers")
   class MailerWorker:
       async def process(self, job):
           email = job.args["email"]
           await send_email(email)
   ```

3. Enqueue a job:

   ```python
   await MailerWorker.enqueue({"email": {"to": "user@example.com", "body": "Hello!"}})
   ```

4. Start processing jobs:

   ```python
   async with oban:
       await asyncio.Event().wait()  # Run until interrupted
   ```

The magic happens! Oban executes the job when there is available bandwidth in the
`mailers` queue.

<!-- INDEX END -->

## Also Available

- [Oban for Elixir][oban-elixir] — The original Oban, with support for PostgreSQL, MySQL, and SQLite3

[oban-elixir]: https://github.com/oban-bg/oban

## Community

There are a few places to connect and communicate with other Oban users:

- [Request an invitation][invite] and join the *#oban* channel on Slack
- Learn about bug reports and upcoming features in the [issue tracker][issues]

[invite]: https://elixir-slack.community/
[issues]: https://github.com/oban-bg/oban-py/issues

## Contributing

To run the Oban test suite you must have PostgreSQL 12+ running. Follow these steps to create the
database and run all tests:

```bash
make test
```

To ensure a commit passes CI you should run `make ci`, or run these checks locally:

* Lint with Ruff (`uv run ruff check`)
* Check formatting (`uv run ruff format --check`)
* Check types (`uv run ty check`)
* Run tests (`uv run pytest`)
