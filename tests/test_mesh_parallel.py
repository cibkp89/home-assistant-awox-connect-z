"""Shared gateway and concurrent 1911 target contracts; BLE is simulated."""
import asyncio
import time
import random
import unittest
from unittest.mock import patch, AsyncMock

import test_mesh as mesh
from awox112.mesh.models import Route


class ParallelTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = mesh.MeshTests.asyncSetUp
    asyncTearDown = mesh.MeshTests.asyncTearDown
    client = mesh.MeshTests.client
    learn = mesh.MeshTests.learn
    executor = mesh.MeshTests.executor
    command = mesh.MeshTests.command
    prepare = mesh.MeshTests.prepare

    async def send(self, client, target=None, *, on=False):
        manager, executor = self.executor(client, on=on)
        manager.submit(target or mesh.fx.T(power=True))
        await executor.async_execute_group()
        return executor

    def both_directions(self):
        self.b._availability.advertisement_time=time.monotonic()
        self.learn(); self.learn(Route(self.b.mac,self.a.mac,2,1))

    async def wait_waiters(self, gateway, count):
        async def wait():
            while True:
                record=self.runtime.pool.preparations.get(gateway)
                if record is not None and len(record[1]) == count:return
                await asyncio.sleep(0)
        await asyncio.wait_for(wait(),1)

    async def test_two_simultaneous_cold_targets_share_one_preparation_and_connection(self):
        self.both_directions()
        for _,state in self.lamps.values():state['power']=False
        entered,release=asyncio.Event(),asyncio.Event()
        original=self.a._async_ensure_connected
        calls=[]
        async def connect(**kw):
            calls.append(1); entered.set(); await release.wait(); await original(**kw)
        self.a._async_ensure_connected=connect
        first=asyncio.create_task(self.send(self.a))
        await entered.wait()
        second=asyncio.create_task(self.send(self.b))
        await self.wait_waiters(self.a.mac,2)
        self.assertNotIn(2,self.radios)
        release.set(); await asyncio.gather(first,second)
        self.assertEqual(calls,[1])
        self.assertEqual(set(self.radios),{1})
        self.assertEqual({mid for mid,_,_ in self.radios[1].writes},{1,2})
        self.assertTrue(all(state['power'] for _,state in self.lamps.values()))
        self.assertFalse(self.runtime.pool.preparations)
        self.assertFalse(self.runtime.pool.users)

    async def test_parallel_unknown_state_queries_keep_target_identity(self):
        self.both_directions()
        first,second=await asyncio.gather(self.a.mesh_dispatch.async_initial_status(),
                                         self.b.mesh_dispatch.async_initial_status())
        self.assertEqual([first.mesh_id,second.mesh_id],[1,2])
        self.assertEqual(set(self.radios),{1})
        self.assertEqual(first.outer_type,0x80)
        self.assertEqual(second.outer_type,0x82)
        self.assertIsNone(self.a.last_mesh_gateway)
        self.assertIsNone(self.b.last_mesh_gateway)

    async def _entity_pair(self, *, known):
        self.both_directions()
        entities=[]
        for client in (self.a,self.b):
            self.lamps[client.configured_mesh_id][1]['power']=False
            entity=mesh.fx.AwoxConnectZLight(client,{'mesh_id':client.configured_mesh_id},
                                            minimum_command_interval_ms=250)
            entity.hass=self.hass
            entity.async_write_ha_state=lambda:None
            if known:entity._manager.set_state(mesh.fx.S(True,False,None))
            entities.append(entity)
        await asyncio.wait_for(asyncio.gather(*(e.async_turn_on() for e in entities)),2)
        self.assertEqual(set(self.radios),{1})
        self.assertTrue(all(e._attr_is_on for e in entities))
        self.assertTrue(all(not e._manager.pending_generations for e in entities))
        self.assertFalse(self.runtime.pool.users)
        await asyncio.wait_for(asyncio.gather(*(e.async_turn_off() for e in entities)),2)
        self.assertTrue(all(not e._attr_is_on for e in entities))
        self.assertEqual(set(self.radios),{1})
        for entity in entities:
            if entity._command_worker is not None:await entity._command_worker

    async def test_real_light_entity_workers_share_cold_and_warm_control(self):
        await self._entity_pair(known=True)

    async def test_real_light_entity_workers_share_unknown_state_bootstrap(self):
        await self._entity_pair(known=False)

    async def test_parallel_sequence_matrix_cold_warm_and_both_arrival_orders(self):
        targets=[mesh.fx.T(power=True),mesh.fx.T(power=False),mesh.fx.T(color=(203,80)),
                 mesh.fx.T(cct=2700),mesh.fx.T(brightness=1),mesh.fx.T(brightness=255),
                 mesh.fx.T(effect='Candle'),mesh.fx.T(effect='Color cycle')]
        cases=0
        for on in (False,True):
            for target in targets:
                for warm in (False,True):
                    for reverse in (False,True):
                        with self.subTest(on=on,target=target,warm=warm,reverse=reverse):
                            for client in (self.a,self.b):await client._async_disconnect()
                            self.radios.clear()
                            self.both_directions()
                            for _,state in self.lamps.values():
                                state.update(power=on,effect=None,color_mode=False,mired=250,
                                             hue=0,saturation=64,brightness=127)
                            if warm:await self.prepare()
                            clients=(self.b,self.a) if reverse else (self.a,self.b)
                            results=await asyncio.gather(*(self.send(c,target,on=on) for c in clients))
                            self.assertLessEqual(len(self.radios),1)
                            for client,result in zip(clients,results):
                                if result.last_confirmed_state is not None:
                                    self.assertEqual(result.last_confirmed_state.mesh_id,client.configured_mesh_id)
                            self.assertFalse(self.runtime.pool.users)
                            self.assertFalse(self.runtime.pool.preparations)
                            cases+=1
        self.assertEqual(cases,64)

    async def test_reverse_arrival_order_can_share_the_other_gateway(self):
        self.both_directions()
        entered,release=asyncio.Event(),asyncio.Event()
        original=self.b._async_ensure_connected
        async def connect(**kw):entered.set(); await release.wait(); await original(**kw)
        self.b._async_ensure_connected=connect
        first=asyncio.create_task(self.command(client=self.b)); await entered.wait()
        second=asyncio.create_task(self.command(client=self.a))
        await self.wait_waiters(self.b.mac,2)
        release.set(); await asyncio.gather(first,second)
        self.assertEqual(set(self.radios),{2})
        self.assertEqual(self.a.last_mesh_gateway,2)

    async def test_warm_target_sends_before_other_target_confirmation(self):
        self.both_directions(); await self.prepare()
        radio=self.radios[1]
        a_written,b_written,allow_a=asyncio.Event(),asyncio.Event(),asyncio.Event()
        emit=radio.emit
        def gated(mid,**kw):
            if mid!=1 or allow_a.is_set():emit(mid,**kw)
        radio.emit=gated
        radio.hook=lambda mid:(a_written if mid==1 else b_written).set()
        with patch.object(mesh.fx.cc,'CONFIRMATION_POLL_WINDOW_MS',500):
            first=asyncio.create_task(self.send(self.a,mesh.fx.T(power=False),on=True))
            await a_written.wait()
            second=asyncio.create_task(self.send(self.b,mesh.fx.T(power=False),on=True))
            try:
                await asyncio.wait_for(b_written.wait(),.3)
                self.assertFalse(first.done())
                await second
            finally:
                allow_a.set(); radio.emit(1)
                await asyncio.gather(first,second,return_exceptions=True)
        self.assertIsNone(first.exception())
        self.assertIsNone(second.exception())
        self.assertFalse(self.runtime.pool.users)

    async def test_failure_in_shared_preparation_can_share_one_fallback(self):
        self.both_directions()
        entered,release=asyncio.Event(),asyncio.Event()
        async def fail(**kw):entered.set(); await release.wait(); raise ConnectionError('first gateway failed')
        self.a._async_ensure_connected=fail
        first=asyncio.create_task(self.command(client=self.a)); await entered.wait()
        second=asyncio.create_task(self.command(client=self.b))
        await self.wait_waiters(self.a.mac,2)
        release.set(); await asyncio.gather(first,second)
        self.assertEqual(set(self.radios),{2})
        self.assertEqual({mid for mid,_,_ in self.radios[2].writes},{1,2})
        self.assertFalse(self.runtime.pool.preparations)

    async def test_unload_gateway_cancels_all_shared_waiters_before_writes(self):
        self.both_directions()
        entered=asyncio.Event()
        async def connect(**kw):entered.set(); await asyncio.Event().wait()
        self.a._async_ensure_connected=connect
        first=asyncio.create_task(self.command(client=self.a)); await entered.wait()
        second=asyncio.create_task(self.command(client=self.b))
        await self.wait_waiters(self.a.mac,2)
        await asyncio.wait_for(self.runtime.remove('entry'),1)
        await asyncio.gather(first,second,return_exceptions=True)
        self.assertTrue(first.cancelled() and second.cancelled())
        self.assertFalse(self.radios)
        self.assertFalse(self.runtime.pool.preparations)
        self.assertFalse(self.runtime.pool.users)
        self.assertEqual(self.a._status_pins,0)

    async def test_unload_only_target_keeps_shared_gateway_for_remaining_target(self):
        await self.runtime.remove('entry')
        await self.runtime.add('gateway',[(self.a,{})],True)
        await self.runtime.add('target',[(self.b,{})],True)
        self.both_directions()
        entered,release=asyncio.Event(),asyncio.Event()
        original=self.a._async_ensure_connected
        async def connect(**kw):entered.set(); await release.wait(); await original(**kw)
        self.a._async_ensure_connected=connect
        first=asyncio.create_task(self.command(client=self.a)); await entered.wait()
        second=asyncio.create_task(self.command(client=self.b))
        await self.wait_waiters(self.a.mac,2)
        await asyncio.wait_for(self.runtime.remove('target'),1)
        await asyncio.gather(second,return_exceptions=True)
        self.assertFalse(self.runtime.pool.preparations[self.a.mac][0].done())
        release.set(); await first
        self.assertEqual([mid for mid,_,_ in self.radios[1].writes],[1])

    async def test_unconfirmed_target_does_not_block_or_fail_other_target(self):
        self.both_directions(); await self.prepare()
        radio=self.radios[1]; emit=radio.emit
        radio.emit=lambda mid,**kw:emit(mid,**kw) if mid==2 else None
        results=await asyncio.gather(self.command(client=self.a),self.command(client=self.b),return_exceptions=True)
        self.assertIsInstance(results[0],mesh.fx.CommandExecutionFailed)
        self.assertIsInstance(results[1],tuple)
        self.assertEqual([mid for mid,_,_ in radio.writes].count(1),2)
        self.assertEqual([mid for mid,_,_ in radio.writes].count(2),1)
        self.assertEqual(radio.polls.count(1),10)
        self.assertEqual(self.b.last_mesh_gateway,1)
        self.assertTrue(self.a.connected)

    async def test_repeat_belongs_only_to_the_target_with_wrong_state(self):
        self.both_directions(); await self.prepare()
        radio=self.radios[1]
        first=True
        def hook(mid):
            nonlocal first
            if mid==1 and first:
                self.lamps[1][1]['power']=True
                first=False
        radio.hook=hook
        await asyncio.gather(self.command(client=self.a),self.command(client=self.b))
        self.assertEqual([mid for mid,_,_ in radio.writes].count(1),2)
        self.assertEqual([mid for mid,_,_ in radio.writes].count(2),1)
        self.assertTrue(all(not state['power'] for _,state in self.lamps.values()))

    async def test_another_target_can_send_during_fade_and_minimum_spacing(self):
        self.both_directions(); await self.prepare()
        radio=self.radios[1]; first_written=asyncio.Event()
        radio.hook=lambda mid:first_written.set() if mid==1 else None
        manager=mesh.fx.CommandManager(mesh.fx.S(True,True,None),minimum_interval_ms=250)
        executor=mesh.fx.CommandExecutor(self.a.mesh_dispatch,manager,mesh_id=1,default_transition=.2)
        manager.submit(mesh.fx.T(color=(120,50),effect='Candle'))
        first=asyncio.create_task(executor.async_execute_group())
        await first_written.wait()
        await asyncio.wait_for(self.command(client=self.b),.3)
        self.assertFalse(first.done())
        await first
        writes=radio.writes
        self.assertEqual(writes[0][0],1)
        self.assertEqual(writes[1][0],2)
        own=[timestamp for mid,_,timestamp in writes if mid==1]
        self.assertGreaterEqual(own[1]-own[0],.49)
        self.assertGreaterEqual(own[2]-own[1],.24)

    async def test_one_physical_write_slot_still_allows_parallel_reply_waits(self):
        self.both_directions()
        # Concurrency=1 serializes individual physical operations, not replies.
        semaphore=asyncio.Semaphore(1)
        self.a._command_semaphore=self.b._command_semaphore=semaphore
        await asyncio.gather(self.command(client=self.a),self.command(client=self.b))
        self.assertEqual(len(self.radios),1)
        self.assertEqual(len(next(iter(self.radios.values())).writes),2)

    async def test_three_cold_targets_choose_one_gateway_without_batch_delay(self):
        self.both_directions()
        third=self.client(3,visible=True)
        await self.runtime.add('third',[(third,{})],True)
        self.learn(Route(self.a.mac,third.mac,1,3))
        entered,release=asyncio.Event(),asyncio.Event()
        original=self.a._async_ensure_connected
        async def connect(**kw):entered.set(); await release.wait(); await original(**kw)
        self.a._async_ensure_connected=connect
        tasks=[asyncio.create_task(self.send(self.a,mesh.fx.T(power=False),on=True))]
        await entered.wait()
        tasks.extend(asyncio.create_task(self.send(c,mesh.fx.T(power=False),on=True)) for c in (self.b,third))
        await self.wait_waiters(self.a.mac,3)
        release.set(); await asyncio.gather(*tasks)
        self.assertEqual(set(self.radios),{1})
        self.assertEqual({mid for mid,_,_ in self.radios[1].writes},{1,2,3})

    async def test_without_learned_route_targets_do_not_share_gateway(self):
        self.b._availability.advertisement_time=time.monotonic()
        await asyncio.gather(self.command(client=self.a),self.command(client=self.b))
        self.assertEqual(set(self.radios),{1,2})

    async def test_explicit_direct_preference_overrides_shared_preparation(self):
        self.both_directions()
        self.runtime.state.set_preferences(self.b.mac,2,self.b.mac,[],{self.b.mac:2})
        await asyncio.gather(self.command(client=self.a),self.command(client=self.b))
        self.assertEqual(set(self.radios),{1,2})

    async def test_blocked_gateway_cannot_be_shared(self):
        self.both_directions()
        self.runtime.state.set_preferences(self.b.mac,2,None,[self.a.mac],{self.a.mac:1})
        await asyncio.gather(self.command(client=self.a),self.command(client=self.b))
        self.assertEqual(set(self.radios),{1,2})

    async def test_stale_gateway_is_an_alternative_after_real_preparation_failure(self):
        self.both_directions()
        self.b._availability.advertisement_time=None
        self.a._async_ensure_connected=AsyncMock(side_effect=ConnectionError('failed gateway'))
        await self.command(client=self.a)
        self.assertEqual(set(self.radios),{2})
        self.assertEqual(self.a.last_mesh_gateway,2)

    async def test_stale_preferred_direct_target_can_override_ready_mesh_path(self):
        self.both_directions();await self.prepare()
        self.b._availability.advertisement_time=None
        self.runtime.state.set_preferences(self.b.mac,2,self.b.mac,[],{self.b.mac:2})
        self.assertFalse(self.runtime.routing.usable(self.b))
        await self.command(client=self.b)
        self.assertIn(2,self.radios)

    async def test_candidates_do_not_publish_unproven_mesh_availability(self):
        self.learn()
        self.a._availability.advertisement_time=None
        self.assertFalse(self.runtime.routing.mesh_routes(self.b.mac))
        self.assertEqual(self.runtime.routing.status(self.b.mac),'unreachable')
        self.assertIn(self.route,self.runtime.routing.candidates(self.b.mac))
        self.assertEqual(self.runtime.routing.status(self.b.mac),'unreachable')

    async def test_disabled_mesh_target_remains_direct_even_during_shared_setup(self):
        self.both_directions()
        self.runtime.enabled.remove(self.b.mac)
        await asyncio.gather(self.command(client=self.a),self.command(client=self.b))
        self.assertEqual(set(self.radios),{1,2})

    async def test_cancel_one_waiter_keeps_preparation_for_other_target(self):
        self.both_directions()
        entered,release=asyncio.Event(),asyncio.Event()
        original=self.a._async_ensure_connected
        async def connect(**kw):entered.set(); await release.wait(); await original(**kw)
        self.a._async_ensure_connected=connect
        first=asyncio.create_task(self.command(client=self.a))
        await entered.wait()
        second=asyncio.create_task(self.command(client=self.b))
        await self.wait_waiters(self.a.mac,2)
        first.cancel(); await asyncio.gather(first,return_exceptions=True)
        self.assertFalse(self.runtime.pool.preparations[self.a.mac][0].done())
        release.set(); await second
        self.assertEqual([mid for mid,_,_ in self.radios[1].writes],[2])
        self.assertEqual(self.a._status_pins,0)
        self.assertFalse(self.runtime.pool.preparations)

    async def test_cancel_all_waiters_cleans_shared_task_and_pins(self):
        self.both_directions()
        entered=asyncio.Event()
        async def connect(**kw):entered.set(); await asyncio.Event().wait()
        self.a._async_ensure_connected=connect
        first=asyncio.create_task(self.command(client=self.a))
        await entered.wait()
        second=asyncio.create_task(self.command(client=self.b))
        await self.wait_waiters(self.a.mac,2)
        first.cancel(); second.cancel()
        await asyncio.gather(first,second,return_exceptions=True)
        self.assertFalse(self.runtime.pool.preparations)
        self.assertFalse(self.runtime.pool.users)
        self.assertFalse(self.runtime.pool.control_users)
        self.assertEqual(self.a._status_pins,0)

    async def test_one_exclusive_probe_blocks_controls_until_release(self):
        entered,release=asyncio.Event(),asyncio.Event()
        async def hold():
            async with self.runtime.pool.reserve(self.a.mac,self.a.mac):
                entered.set(); await release.wait()
        holder=asyncio.create_task(hold()); await entered.wait()
        task=asyncio.create_task(self.command(client=self.a))
        await asyncio.sleep(.005)
        self.assertFalse(self.radios)
        release.set(); await asyncio.gather(holder,task)

    async def test_queued_exclusive_probe_is_not_starved_by_later_controls(self):
        pool=self.runtime.pool
        entered,release=asyncio.Event(),asyncio.Event()
        sequence=[]
        async def first():
            async with pool.control(self.a.mac,self.a.mac):entered.set(); await release.wait()
        async def exclusive():
            async with pool.reserve(self.a.mac,self.b.mac):sequence.append('probe')
        async def later():
            async with pool.control(self.a.mac,self.b.mac):sequence.append('later')
        owner=asyncio.create_task(first()); await entered.wait()
        writer=asyncio.create_task(exclusive()); await asyncio.sleep(0)
        reader=asyncio.create_task(later()); await asyncio.sleep(0)
        release.set(); await asyncio.gather(owner,writer,reader)
        self.assertEqual(sequence,['probe','later'])

    async def test_cancelled_exclusive_waiter_does_not_block_future_controls(self):
        pool=self.runtime.pool
        entered,release=asyncio.Event(),asyncio.Event()
        async def first():
            async with pool.control(self.a.mac,self.a.mac):entered.set(); await release.wait()
        async def exclusive():
            async with pool.reserve(self.a.mac,self.b.mac):self.fail('cancelled')
        owner=asyncio.create_task(first()); await entered.wait()
        writer=asyncio.create_task(exclusive()); await asyncio.sleep(0)
        writer.cancel(); await asyncio.gather(writer,return_exceptions=True)
        async with asyncio.timeout(.2):
            async with pool.control(self.a.mac,self.b.mac):pass
        release.set(); await owner

    async def test_same_target_stays_serial_even_across_different_gateways(self):
        pool=self.runtime.pool
        entered,release=asyncio.Event(),asyncio.Event()
        async def first():
            async with pool.control(self.a.mac,self.b.mac):entered.set(); await release.wait()
        acquired=asyncio.Event()
        async def second():
            async with pool.control(self.b.mac,self.b.mac):acquired.set()
        owner=asyncio.create_task(first()); await entered.wait()
        waiter=asyncio.create_task(second()); await asyncio.sleep(.005)
        self.assertFalse(acquired.is_set())
        release.set(); await asyncio.gather(owner,waiter)
        self.assertTrue(acquired.is_set())

    async def test_target_wait_does_not_reserve_another_gateway_reader(self):
        pool=self.runtime.pool
        entered,release=asyncio.Event(),asyncio.Event()
        async def first():
            async with pool.control(self.b.mac,self.b.mac):entered.set(); await release.wait()
        async def second():
            async with pool.control(self.a.mac,self.b.mac):pass
        owner=asyncio.create_task(first()); await entered.wait()
        waiter=asyncio.create_task(second()); await asyncio.sleep(0)
        try:
            async with asyncio.timeout(.2):
                async with pool.reserve(self.a.mac,self.a.mac):pass
            self.assertFalse(waiter.done())
        finally:
            release.set(); await asyncio.gather(owner,waiter)

    async def test_repeated_leases_reuse_gateway_and_target_synchronizers(self):
        pool=self.runtime.pool
        async with pool.control(self.a.mac,self.b.mac):pass
        gate,target=pool.locks[self.a.mac],pool.target_locks[self.b.mac]
        for _ in range(20):
            async with pool.control(self.a.mac,self.b.mac):
                self.assertIs(pool.locks[self.a.mac],gate)
                self.assertIs(pool.target_locks[self.b.mac],target)
        self.assertEqual(len(pool.locks),1)
        self.assertEqual(len(pool.target_locks),1)

    async def test_mixed_reservations_and_cancellations_keep_mutual_exclusion(self):
        pool=self.runtime.pool
        rng=random.Random(11320261029)
        active={self.a.mac:[],self.b.mac:[]};targets=set()
        async def worker(index,gateway,target,shared):
            context=pool.control(gateway,target) if shared else pool.reserve(gateway,target)
            async with context:
                current=active[gateway]
                if not shared:self.assertFalse(current)
                else:
                    self.assertNotIn(False,current)
                    self.assertNotIn(target,targets)
                    targets.add(target)
                current.append(shared)
                try:await asyncio.sleep(0)
                finally:
                    current.remove(shared)
                    if shared:targets.remove(target)
        tasks=[asyncio.create_task(worker(i,rng.choice((self.a.mac,self.b.mac)),
                                          rng.choice((self.a.mac,self.b.mac)),i%4!=0))
               for i in range(120)]
        await asyncio.sleep(0);await asyncio.sleep(0)
        for task in tasks[::5]:task.cancel()
        results=await asyncio.wait_for(asyncio.gather(*tasks,return_exceptions=True),2)
        self.assertTrue(all(r is None or isinstance(r,asyncio.CancelledError) for r in results),results)
        self.assertFalse(any(active.values()))
        self.assertFalse(targets)
        self.assertFalse(pool.users)
        self.assertEqual(self.a._status_pins+self.b._status_pins,0)

    async def test_independent_gateway_controls_do_not_wait_for_other_gateway(self):
        entered,release=asyncio.Event(),asyncio.Event()
        async def first():
            async with self.runtime.pool.control(self.a.mac,self.a.mac):entered.set(); await release.wait()
        owner=asyncio.create_task(first()); await entered.wait()
        await asyncio.wait_for(self.command(client=self.b),.2)
        self.assertFalse(owner.done())
        release.set(); await owner


if __name__=='__main__':unittest.main(verbosity=2)
