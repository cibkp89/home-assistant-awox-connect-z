"""Five fresh learning sessions and neutral cancellation ranking."""
import asyncio
import unittest
from unittest.mock import patch
import test_mesh as mesh
import test_mesh_ui as ui
from awox112.mesh.models import LearningResult, Route
from awox112.mesh import MeshRuntime


class FiveRounds(unittest.IsolatedAsyncioTestCase):
    asyncSetUp=mesh.MeshTests.asyncSetUp
    asyncTearDown=mesh.MeshTests.asyncTearDown
    client=mesh.MeshTests.client
    learn=mesh.MeshTests.learn
    prepare=mesh.MeshTests.prepare

    async def test_five_fresh_sessions_and_no_lamp_commands(self):
        connects=[]
        original=self.a._async_ensure_connected
        async def connect(**kwargs):
            if not self.a.connected:connects.append(1)
            await original(**kwargs)
        with patch.object(self.a,'_async_ensure_connected',connect):
            await self.runtime.learning.run({self.b.mac})
        result,=self.runtime.learning.results
        self.assertEqual(len(connects),5)
        self.assertEqual((result.attempts,result.successes),(5,5))
        self.assertEqual((self.runtime.learning.completed,self.runtime.learning.total),(5,5))
        self.assertFalse(self.a.connected)
        self.assertFalse(self.runtime.pool.users)
        self.assertFalse(self.radios[1].writes)
        self.assertFalse(self.runtime.state.last_gateways)

    async def test_one_connection_per_gateway_round_for_multiple_targets(self):
        third=self.client(3,visible=False)
        await self.runtime.add('third',[(third,{})],True)
        connects=[];original=self.a._async_ensure_connected
        async def connect(**kwargs):
            if not self.a.connected:connects.append(1)
            await original(**kwargs)
        with patch.object(self.a,'_async_ensure_connected',connect):
            await self.runtime.learning.run({self.b.mac,third.mac})
        self.assertEqual(len(connects),5)
        self.assertEqual(len(self.runtime.learning.results),2)
        self.assertEqual(self.runtime.learning.completed,10)
        self.assertTrue(all(r.successes==5 for r in self.runtime.learning.results))

    async def test_four_successes_survive_failed_last_sample_and_reload(self):
        count=0
        async def probe(route,**kwargs):
            nonlocal count
            count+=1
            return LearningResult(route,count!=5,1,.01,None if count!=5 else 'No valid target reply')
        with patch.object(self.runtime.learning,'probe',probe):
            await self.runtime.learning.run({self.b.mac})
        result,=self.runtime.learning.results
        self.assertEqual((result.success,result.successes,result.attempts),(True,4,5))
        self.assertIn((self.a.mac,self.b.mac),self.runtime.state.routes)
        other=MeshRuntime(self.hass,'network');await other.storage.load()
        self.assertEqual(other.state.learning_results[(self.a.mac,self.b.mac)],result)

    async def test_all_five_missing_replies_revoke_old_route(self):
        self.learn()
        async def probe(route,**kwargs):return LearningResult(route,False,7,.01,'No valid target reply')
        with patch.object(self.runtime.learning,'probe',probe):
            await self.runtime.learning.run({self.b.mac})
        result,=self.runtime.learning.results
        self.assertEqual((result.attempts,result.successes,result.polls),(5,0,35))
        self.assertNotIn((self.a.mac,self.b.mac),self.runtime.state.routes)

    async def test_failed_preparation_does_not_abort_remaining_rounds(self):
        original=self.runtime.pool.prepare;count=0
        async def prepare(client):
            nonlocal count
            count+=1
            if count==1:raise ConnectionError('injected preparation failure')
            return await original(client)
        with patch.object(self.runtime.pool,'prepare',prepare):
            await self.runtime.learning.run({self.b.mac})
        result,=self.runtime.learning.results
        self.assertEqual((result.attempts,result.successes),(5,4))
        self.assertIsNone(self.runtime.learning.error)

    async def test_abort_during_probe_keeps_partial_counts_and_releases_gateway(self):
        entered=asyncio.Event();count=0
        async def probe(route,**kwargs):
            nonlocal count
            count+=1
            if count==2:entered.set();await asyncio.Event().wait()
            return LearningResult(route,True,1,.01)
        with patch.object(self.runtime.learning,'probe',probe):
            self.runtime.learning.start({self.b.mac});await entered.wait()
            await self.runtime.learning.stop()
        result,=self.runtime.learning.results
        self.assertEqual((result.attempts,result.successes),(1,1))
        self.assertFalse(self.a.connected)
        self.assertFalse(self.runtime.pool.users)
        self.assertEqual(self.a._status_pins,0)

    async def test_cancelled_samples_do_not_reach_minimum_or_reduce_quality(self):
        self.learn()
        for _ in range(4):self.runtime.state.note_ble(self.a.mac,1,'successful',10,-50)
        self.runtime.state.note_ble(self.a.mac,1,'cancelled',1,-50)
        self.assertEqual(self.runtime.routing._candidate_key(self.route)[4:6],(1,0))
        self.runtime.state.note_ble(self.a.mac,1,'successful',10,-50)
        self.assertEqual(self.runtime.routing._candidate_key(self.route)[4:6],(0,-1))

    async def test_history_cannot_override_better_measured_gateway(self):
        third=self.client(3,visible=True)
        await self.runtime.add('third',[(third,{})],True)
        self.learn();self.learn(Route(third.mac,self.b.mac,3,2))
        self.runtime.state.last_gateways[self.b.mac]=1
        for _ in range(5):
            self.runtime.state.note_ble(self.a.mac,1,'successful',200,-50)
            self.runtime.state.note_ble(third.mac,3,'successful',50,-60)
        self.assertEqual(self.runtime.routing.candidates(self.b.mac)[0].gateway,third.mac)

    async def test_overview_and_progress_display_aggregate_counts(self):
        self.learn()
        self.runtime.state.learning_results[(self.a.mac,self.b.mac)]=LearningResult(
            self.route,True,5,.05,None,5,4)
        flow=ui.ManagementTests.flow(self)
        self.assertIn('(4/5)',flow._mesh_summary())
        learner=self.runtime.learning
        learner.round=3;learner.completed=2;learner.total=5
        self.assertIn('3/5',flow._mesh_progress_text())
        self.assertIn('2/5',flow._mesh_progress_text())

    async def test_invalid_persisted_aggregate_counts_are_rejected(self):
        from dataclasses import asdict
        value=asdict(LearningResult(self.route,True,5,.05,None,5,4))
        for fields in ({'attempts':0},{'attempts':6},{'attempts':True},
                       {'successful_attempts':6},{'successful_attempts':True},
                       {'successful_attempts':-1},{'successful_attempts':0}):
            with self.subTest(fields=fields):
                mesh.MemoryStore.data[self.runtime.storage.store.key]={'learning_results':[dict(value,**fields)]}
                await self.runtime.storage.load()
                self.assertFalse(self.runtime.state.learning_results)


if __name__=='__main__':unittest.main(verbosity=2)
