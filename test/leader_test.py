import pytest


class TestLeadership:
    @pytest.mark.oban(leadership=True)
    async def test_single_instance_becomes_leader(self, oban_instance):
        async with oban_instance() as oban:
            assert oban.is_leader

    @pytest.mark.oban(leadership=False)
    async def test_instance_with_leadership_disabled(self, oban_instance):
        async with oban_instance() as oban:
            assert not oban.is_leader

    @pytest.mark.oban(queues={})
    async def test_client_mode_does_not_elect_leader(self, oban_instance):
        async with oban_instance() as oban:
            assert not oban.is_leader

    @pytest.mark.oban(leadership=True)
    async def test_multiple_instances_elect_single_leader(self, oban_instance):
        oban_1 = oban_instance()
        oban_2 = oban_instance()

        await oban_1.start()
        await oban_2.start()

        try:
            leaders = [oban_1.is_leader, oban_2.is_leader]

            assert list(filter(None, leaders)) == [True]
        finally:
            await oban_1.stop()
            await oban_2.stop()

    @pytest.mark.oban(leadership=True)
    async def test_leader_resigns_on_stop(self, oban_instance):
        oban_1 = oban_instance()
        oban_2 = oban_instance()

        try:
            await oban_1.start()

            assert oban_1.is_leader
            assert not oban_2.is_leader

            await oban_1.stop()
            await oban_2.start()

            assert oban_2.is_leader
        finally:
            await oban_2.stop()
