import pytest


async def all_producers(conn):
    result = await conn.execute("""
        SELECT uuid, name, node, queue, meta
        FROM oban_producers
        ORDER BY queue
    """)

    return await result.fetchall()


class TestProducerTracking:
    @pytest.mark.oban(node="work-1", queues={"alpha": 1, "gamma": 2})
    async def test_producer_records_created_on_start(self, oban_instance):
        async with oban_instance() as oban:
            async with oban._connection() as conn:
                alpha, gamma = await all_producers(conn)

                assert alpha[0]
                assert alpha[1] == "oban"
                assert alpha[2] == "work-1"
                assert alpha[3] == "alpha"
                assert alpha[4]["local_limit"] == 1

                assert gamma[0]
                assert gamma[1] == "oban"
                assert gamma[2] == "work-1"
                assert gamma[3] == "gamma"
                assert gamma[4]["local_limit"] == 2

    @pytest.mark.oban(queues={"alpha": 1})
    async def test_producer_records_deleted_on_stop(self, oban_instance):
        oban = oban_instance()

        await oban.start()

        async with oban._connection() as conn:
            records = await all_producers(conn)

            assert len(records) == 1

        await oban.stop()

        async with oban._connection() as conn:
            records = await all_producers(conn)

            assert len(records) == 0
