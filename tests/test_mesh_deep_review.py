"""Adversarial lifecycle and multi-gateway capacity tests; simulated transport."""
import asyncio
import unittest
from unittest.mock import AsyncMock,patch

import test_mesh as mesh
from awox112.client import RuntimeSelectedCommand,RuntimeCommandResult,RuntimeCommandStatus


class DeepReview(unittest.IsolatedAsyncioTestCase):
    asyncSetUp=mesh.MeshTests.asyncSetUp
    asyncTearDown=mesh.MeshTests.asyncTearDown
    client=mesh.MeshTests.client
    learn=mesh.MeshTests.learn
    executor=mesh.MeshTests.executor
    command=mesh.MeshTests.command
    prepare=mesh.MeshTests.prepare

    async def test_active_direct_write_stops_at_retirement_before_cleanup_yields(self):
        await self.prepare()
        acquired,attempt,retiring,release=(asyncio.Event() for _ in range(4))
        errors=[]
        async def owner():
            async with self.a.mesh_dispatch.status_transaction():
                acquired.set();await attempt.wait()
                try:
                    await self.a.mesh_dispatch.async_send_selected_once(lambda:RuntimeSelectedCommand(
                        mesh.fx.p.make_power(False,mesh_id=1),'test'))
                except Exception as error:errors.append(error)
        async def stop():retiring.set();await release.wait()
        task=asyncio.create_task(owner());await acquired.wait()
        with patch.object(self.runtime.learning,'stop',stop):
            removal=asyncio.create_task(self.runtime.remove('entry'));await retiring.wait()
            try:
                attempt.set();await asyncio.wait_for(task,1)
            finally:
                release.set();await removal
        self.assertEqual(len(errors),1)
        self.assertIsInstance(errors[0],ConnectionError)
        self.assertFalse(self.radios[1].writes)

    async def test_direct_group_rejects_changed_destination_before_write(self):
        await self.prepare()
        error=None
        async with self.a.mesh_dispatch.status_transaction():
            self.a.configured_mesh_id=99
            try:
                await self.a.mesh_dispatch.async_send_selected_once(lambda:RuntimeSelectedCommand(
                    mesh.fx.p.make_power(False,mesh_id=1),'test'))
            except Exception as exc:error=exc
            finally:self.a.configured_mesh_id=1
        self.assertIsInstance(error,ConnectionError)
        self.assertFalse(self.radios[1].writes)

    async def test_queued_preparation_rejects_retired_source_before_connect(self):
        original=self.runtime.clients.pop(self.a.mac)
        operation=AsyncMock(return_value=RuntimeCommandResult(RuntimeCommandStatus.NOT_SELECTED))
        error=None
        try:
            with patch.object(self.a,'async_send_selected_once',operation):
                try:await self.runtime.pool.prepare(self.a)
                except Exception as exc:error=exc
        finally:self.runtime.clients[self.a.mac]=original
        self.assertIsInstance(error,ConnectionError)
        operation.assert_not_awaited()

    async def test_same_gateway_queue_does_not_consume_other_gateway_capacity(self):
        for queued_kind in ('selected','poll','repeat','initial','verify'):
            with self.subTest(queued_kind=queued_kind):
                await self.prepare(self.a);await self.prepare(self.b)
                semaphore=asyncio.Semaphore(2)
                self.a._command_semaphore=self.b._command_semaphore=semaphore
                entered,release=asyncio.Event(),asyncio.Event()
                radio=self.radios[1];write=radio.write_gatt_char
                async def held(*args,**kwargs):
                    entered.set();await release.wait();return await write(*args,**kwargs)
                def select():return RuntimeSelectedCommand(mesh.fx.p.make_power(False,mesh_id=1),'test')
                with patch.object(radio,'write_gatt_char',held):
                    first=asyncio.create_task(self.a.async_send_selected_once(select,expected_session=self.a._session_generation))
                    await entered.wait()
                    gen=self.a._session_generation
                    if queued_kind=='selected':queued=self.a.async_send_selected_once(lambda:None)
                    elif queued_kind=='poll':queued=self.a.async_poll_status(gen)
                    elif queued_kind=='repeat':queued=self.a.async_repeat_plaintext(mesh.fx.p.make_power(False,mesh_id=1),gen,lambda:True)
                    elif queued_kind=='initial':queued=self.a.async_initial_status()
                    else:queued=self.a.async_verify_mesh_credentials()
                    second=asyncio.create_task(queued);await asyncio.sleep(0)
                    other=asyncio.create_task(self.b.async_send_selected_once(lambda:RuntimeSelectedCommand(
                        mesh.fx.p.make_power(False,mesh_id=2),'other'),expected_session=self.b._session_generation))
                    try:
                        done,_=await asyncio.wait({other},timeout=.1)
                        progressed=other in done
                    finally:
                        release.set();results=await asyncio.gather(first,second,other,return_exceptions=True)
                self.assertTrue(all(not isinstance(r,BaseException) for r in results),results)
                self.assertTrue(progressed,'A queued operation must not occupy the second global slot')

    async def test_cancellation_waiting_for_global_capacity_releases_client_lock(self):
        for kind in ('selected','poll','repeat','initial','verify'):
            with self.subTest(kind=kind):
                await self.prepare(self.a)
                semaphore=asyncio.Semaphore(1)
                self.a._command_semaphore=semaphore
                await semaphore.acquire()
                gen=self.a._session_generation
                if kind=='selected':operation=self.a.async_send_selected_once(lambda:None)
                elif kind=='poll':operation=self.a.async_poll_status(gen)
                elif kind=='repeat':operation=self.a.async_repeat_plaintext(
                    mesh.fx.p.make_power(False,mesh_id=1),gen,lambda:True)
                elif kind=='initial':operation=self.a.async_initial_status()
                else:operation=self.a.async_verify_mesh_credentials()
                task=asyncio.create_task(operation)
                try:
                    await asyncio.sleep(0)
                    self.assertFalse(task.done())
                    task.cancel()
                    with self.assertRaises(asyncio.CancelledError):await task
                    self.assertFalse(self.a._command_lock.locked())
                    self.assertTrue(self.a.connected)
                finally:
                    semaphore.release()
                    if not task.done():
                        task.cancel();await asyncio.gather(task,return_exceptions=True)
                result=await asyncio.wait_for(self.a.async_send_selected_once(lambda:None),1)
                self.assertIs(result.status,RuntimeCommandStatus.NOT_SELECTED)
                self.assertFalse(self.a._command_lock.locked())

    async def test_registered_object_replacement_rejects_all_old_group_operations(self):
        await self.prepare()
        async with self.a.mesh_dispatch.status_transaction():
            generation=self.a.mesh_dispatch.generation
            self.runtime.clients[self.a.mac]=object()
            try:
                for kind in ('selected','poll','repeat','confirmed'):
                    with self.subTest(kind=kind),self.assertRaises(ConnectionError):
                        if kind=='selected':
                            await self.a.mesh_dispatch.async_send_selected_once(lambda:None)
                        elif kind=='poll':await self.a.mesh_dispatch.async_poll_status(generation)
                        elif kind=='repeat':await self.a.mesh_dispatch.async_repeat_plaintext(
                            mesh.fx.p.make_power(False,mesh_id=1),generation,lambda:True)
                        else:self.a.mesh_dispatch.note_confirmed()
            finally:self.runtime.clients[self.a.mac]=self.a
        self.assertFalse(self.radios[1].writes)

    async def test_independent_gateways_preserve_order_and_global_capacity_under_load(self):
        for capacity in (1,2):
            with self.subTest(capacity=capacity):
                await self.prepare(self.a);await self.prepare(self.b)
                semaphore=asyncio.Semaphore(capacity)
                self.a._command_semaphore=self.b._command_semaphore=semaphore
                active=set();peak=0;observed={1:[],2:[]}
                def wrap(mid,write):
                    async def measured(*args,**kwargs):
                        nonlocal peak
                        self.assertNotIn(mid,active)
                        active.add(mid);peak=max(peak,len(active))
                        self.assertLessEqual(len(active),capacity)
                        try:
                            for _ in range(3):await asyncio.sleep(0)
                            return await write(*args,**kwargs)
                        finally:active.remove(mid)
                    return measured
                def selection(mid,index):
                    observed[mid].append(index)
                    return RuntimeSelectedCommand(mesh.fx.p.make_power(bool(index%2),mesh_id=mid),'load')
                with patch.object(self.radios[1],'write_gatt_char',wrap(1,self.radios[1].write_gatt_char)),patch.object(
                        self.radios[2],'write_gatt_char',wrap(2,self.radios[2].write_gatt_char)):
                    tasks=[asyncio.create_task(client.async_send_selected_once(
                        lambda mid=mid,index=index:selection(mid,index),expected_session=client._session_generation))
                        for index in range(40) for mid,client in ((1,self.a),(2,self.b))]
                    results=await asyncio.wait_for(asyncio.gather(*tasks),5)
                self.assertTrue(all(result.status is RuntimeCommandStatus.SUCCESS for result in results))
                self.assertEqual(observed,{1:list(range(40)),2:list(range(40))})
                self.assertEqual(peak,capacity)
                self.assertFalse(active)
                self.assertFalse(self.a._command_lock.locked())
                self.assertFalse(self.b._command_lock.locked())


if __name__=='__main__':unittest.main(verbosity=2)
