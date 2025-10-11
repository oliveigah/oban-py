import os
import pytest
import pytest_asyncio
import psycopg

from psycopg import AsyncConnection

from oban.schema import install_sql, install, uninstall_sql, uninstall

DB_URL_BASE = os.getenv("DB_URL_BASE", "postgresql://postgres@localhost")


def postgres_conn():
    return psycopg.connect(f"{DB_URL_BASE}/postgres", autocommit=True)


async def list_tables(conn, prefix="public"):
    result = await conn.execute(
        f"""SELECT table_name FROM information_schema.tables
        WHERE table_schema = '{prefix}'
        ORDER BY table_name
        """
    )

    return [row[0] for row in await result.fetchall()]


@pytest_asyncio.fixture
async def isolated_db():
    test_db = "oban_schema_test_temp"

    with postgres_conn() as conn:
        conn.execute(f'DROP DATABASE IF EXISTS "{test_db}"')
        conn.execute(f'CREATE DATABASE "{test_db}"')

    async with await AsyncConnection.connect(f"{DB_URL_BASE}/{test_db}") as conn:
        yield conn

    with postgres_conn() as conn:
        conn.execute(f'DROP DATABASE IF EXISTS "{test_db}"')


class TestInstallSql:
    def test_contains_expected_schema_elements(self):
        sql = install_sql(prefix="isolated")

        assert "CREATE TYPE isolated.oban_job_state" in sql
        assert "CREATE TABLE isolated.oban_jobs" in sql
        assert "CREATE UNLOGGED TABLE isolated.oban_leaders" in sql
        assert "CREATE INDEX" in sql

    def test_scoping_elements_to_the_prefix(self):
        sql = install_sql()

        assert "CREATE TYPE public.oban_job_state" in sql
        assert "CREATE TABLE public.oban_jobs" in sql
        assert "CREATE UNLOGGED TABLE public.oban_leaders" in sql
        assert "CREATE INDEX" in sql


class TestUninstallSql:
    def test_contains_expected_schema_elements(self):
        sql = uninstall_sql()

        assert "DROP TABLE" in sql
        assert "oban_jobs" in sql
        assert "oban_leaders" in sql
        assert "DROP TYPE" in sql
        assert "oban_job_state" in sql


class TestInstall:
    @pytest.mark.asyncio
    async def test_creates_schema_in_database(self, isolated_db):
        await install(isolated_db)

        tables = await list_tables(isolated_db)

        assert "oban_jobs" in tables
        assert "oban_leaders" in tables

    @pytest.mark.asyncio
    async def test_creates_schema_in_database_using_prefix(self, isolated_db):
        await isolated_db.execute("CREATE SCHEMA IF NOT EXISTS isolated")

        await install(isolated_db, prefix="isolated")

        tables = await list_tables(isolated_db, prefix="isolated")

        assert "oban_jobs" in tables
        assert "oban_leaders" in tables


class TestUninstall:
    @pytest.mark.asyncio
    async def test_removes_schema_from_database(self, isolated_db):
        await install(isolated_db)
        await uninstall(isolated_db)

        tables = await list_tables(isolated_db)

        assert "oban_jobs" not in tables
        assert "oban_leaders" not in tables
