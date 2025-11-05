import os

import psycopg
import pytest
from click.testing import CliRunner

from oban.cli import main


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def db_url(test_database):
    base = os.getenv("DB_URL_BASE", "postgresql://postgres@localhost")
    name = "oban_cli_test"

    with psycopg.connect(f"{base}/postgres", autocommit=True) as conn:
        conn.execute(f'DROP DATABASE IF EXISTS "{name}"')
        conn.execute(f'CREATE DATABASE "{name}"')

    yield f"{base}/{name}"

    with psycopg.connect(f"{base}/postgres", autocommit=True) as conn:
        conn.execute(f'DROP DATABASE IF EXISTS "{name}"')


class TestInstallCommand:
    def test_install_creates_schema(self, runner, db_url):
        result = runner.invoke(main, ["install", "--database-url", db_url])
        assert result.exit_code == 0

    def test_install_with_env_var(self, runner, db_url):
        result = runner.invoke(main, ["install"], env={"OBAN_DATABASE_URL": db_url})
        assert result.exit_code == 0

    def test_install_without_database_url_fails(self, runner):
        assert runner.invoke(main, ["install"]).exit_code > 0


class TestUninstallCommand:
    def test_uninstall_removes_schema(self, runner, db_url):
        runner.invoke(main, ["install", "--database-url", db_url])
        result = runner.invoke(main, ["uninstall", "--database-url", db_url])
        assert result.exit_code == 0

    def test_uninstall_with_env_var(self, runner, db_url):
        runner.invoke(main, ["install"], env={"OBAN_DATABASE_URL": db_url})
        result = runner.invoke(main, ["uninstall"], env={"OBAN_DATABASE_URL": db_url})
        assert result.exit_code == 0


class TestStartCommand:
    def test_start_with_env_vars(self, runner, db_url):
        runner.invoke(main, ["install", "--database-url", db_url])

        env = {
            "OBAN_DATABASE_URL": db_url,
            "OBAN_QUEUES": "default:10,mailers:5",
        }

        assert runner.invoke(main, ["start", "--help"], env=env).exit_code == 0

    def test_start_with_params(self, runner, db_url):
        runner.invoke(main, ["install", "--database-url", db_url])

        result = runner.invoke(
            main,
            ["start", "--database-url", db_url, "--queues", "default:10", "--help"],
        )

        assert result.exit_code == 0
