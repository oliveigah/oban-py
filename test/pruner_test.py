import pytest


class TestPruner:
    async def insert_job(self, conn, state, ago):
        ts_field = f"{state}_at"

        rows = await conn.execute(
            f"""
                INSERT INTO oban_jobs (state, worker, {ts_field})
                VALUES (%s, 'Worker', timezone('UTC', now()) - make_interval(secs => %s))
                RETURNING id
                """,
            (state, ago),
        )

        (id,) = await rows.fetchone()

        return id

    async def job_ids(self, conn):
        rows = await conn.execute("SELECT id FROM oban_jobs")
        result = await rows.fetchall()

        return [id for (id,) in result]

    @pytest.mark.oban(leadership=True, pruner={"max_age": 60})
    async def test_pruner_deletes_expired_jobs(self, oban_instance):
        async with oban_instance() as oban:
            # Insert jobs and commit them so pruner can see them
            async with oban._connection() as conn:
                async with conn.transaction():
                    await self.insert_job(conn, "completed", 61)
                    await self.insert_job(conn, "cancelled", 61)
                    await self.insert_job(conn, "cancelled", 61)
                    await self.insert_job(conn, "discarded", 61)

                    id_1 = await self.insert_job(conn, "scheduled", 61)
                    id_2 = await self.insert_job(conn, "completed", 59)
                    id_3 = await self.insert_job(conn, "discarded", 59)

            # Force synchronous pruning
            await oban._pruner._prune()

            async with oban._connection() as conn:
                job_ids = await self.job_ids(conn)

            assert [id_1, id_2, id_3] == job_ids
