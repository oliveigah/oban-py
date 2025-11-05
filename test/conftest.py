import os
import pytest
import pytest_asyncio
import psycopg
import uvloop

from psycopg_pool import AsyncConnectionPool

from oban import Oban
from oban.schema import install
from oban.testing import reset_oban

DB_URL_BASE = os.getenv("DB_URL_BASE", "postgresql://postgres@localhost")


@pytest.fixture(scope="session")
def event_loop_policy():
    return uvloop.EventLoopPolicy()


@pytest_asyncio.fixture(scope="session")
async def test_database(request):
    worker_id = getattr(request.config, "workerinput", {}).get("workerid", "master")

    if worker_id == "master":
        worker_idx = 0
    else:
        worker_idx = int(worker_id.replace("gw", ""))

    dbname = f"oban_py_test_{worker_idx}"
    db_url = f"{DB_URL_BASE}/{dbname}"

    with psycopg.connect(f"{DB_URL_BASE}/postgres", autocommit=True) as conn:
        exists = conn.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s", (dbname,)
        ).fetchone()

        if not exists:
            conn.execute(f'CREATE DATABASE "{dbname}"')

            async with await psycopg.AsyncConnection.connect(db_url) as conn:
                await install(conn)

    yield db_url


@pytest_asyncio.fixture
async def oban_instance(request, test_database):
    mark = request.node.get_closest_marker("oban")
    mark_kwargs = mark.kwargs if mark else {}

    pool = AsyncConnectionPool(conninfo=test_database, min_size=2, open=False)

    await pool.open()
    await pool.wait()

    instances = []

    def _create_instance(**overrides):
        params = {"conn": pool, "leadership": False, "stager": {"interval": 0.01}}
        oban = Oban(**{**params, **mark_kwargs, **overrides})

        instances.append(oban)

        return oban

    yield _create_instance

    for oban in instances:
        await reset_oban(oban)

    await pool.close()
