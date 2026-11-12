"""Mesh management with simulated HA/BLE; no physical hardware claims."""
import asyncio
import copy
import json
from pathlib import Path
import types
import unittest
from unittest.mock import AsyncMock, patch

import test_mesh as mesh
from awox112.mesh import DATA_MESH, MeshRuntime
from awox112.mesh.models import Route, LearningResult

mesh.opt.config_flow.dt_util.as_local = lambda value: value.astimezone()


class ManagementTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = mesh.MeshTests.asyncSetUp
    asyncTearDown = mesh.MeshTests.asyncTearDown
    client = mesh.MeshTests.client
    learn = mesh.MeshTests.learn
    executor = mesh.MeshTests.executor
    command = mesh.MeshTests.command
    prepare = mesh.MeshTests.prepare

    def flow(self, **options):
        self.hass.data[DATA_MESH] = {'network': self.runtime}
        self.hass.config = types.SimpleNamespace(language='de')
        flow = mesh.opt.config_flow.AwoxConnectZOptionsFlow()
        flow.hass = self.hass
        flow.config_entry = types.SimpleNamespace(entry_id='entry', options=options)
        flow.async_create_entry = lambda **kw: dict(type='create_entry', **kw)
        flow.async_show_menu = lambda **kw: dict(type='menu', **kw)
        flow.async_show_progress_done = lambda **kw: dict(type='progress_done', **kw)
        def show(**kw):
            flow.cur_step = dict(type='progress', **kw)
            return flow.cur_step
        flow.async_show_progress = show
        flow.notified = 0
        def notify():flow.notified += 1
        flow.async_notify_flow_changed = notify
        return flow

    def rule(self, *, preferred=None, blocked=()):
        self.runtime.state.set_preferences(self.b.mac, 2, preferred, list(blocked),
            {mac:c.configured_mesh_id for mac,c in self.runtime.clients.items()})

    async def test_menu_has_separate_runtime_and_mesh_without_hardware(self):
        flow = self.flow()
        result = await flow.async_step_init()
        self.assertEqual(result['menu_options'], ['runtime', 'mesh'])
        menu = await flow.async_step_mesh()
        self.assertIn('mesh_matrix', menu['menu_options'])
        self.assertNotIn('mesh_cancel', menu['menu_options'])
        self.assertFalse(self.radios)

    async def test_runtime_save_preserves_mesh_and_removes_old_transient_flag(self):
        flow = self.flow(mesh_enabled=False, mesh_learn=True, idle_disconnect=7)
        result = await flow.async_step_runtime({'default_transition': .7})
        self.assertEqual(result['data'], dict(mesh_enabled=False,idle_disconnect=7,default_transition=.7))

    async def test_mesh_toggle_preserves_runtime_options(self):
        flow = self.flow(default_transition=3, idle_disconnect=7)
        result = await flow.async_step_runtime({'mesh_enabled': False})
        self.assertEqual(result['data'], dict(default_transition=3,idle_disconnect=7,mesh_enabled=False))

    async def test_empty_or_foreign_targets_never_start_learning(self):
        flow = self.flow()
        for values in ([], ['AA:BB:CC:DD:EE:FF'], 'invalid'):
            result = await flow.async_step_mesh_learn({'targets':values})
            self.assertEqual(result['errors']['base'], 'mesh_no_targets')
        self.assertIsNone(self.runtime.learning.task)
        self.assertFalse(self.radios)

    async def test_disabled_entry_cannot_start_learning(self):
        flow = self.flow(mesh_enabled=False)
        result = await flow.async_step_mesh_learn({'targets':[self.b.mac]})
        self.assertEqual(result['errors']['base'],'mesh_not_ready')
        self.assertIsNone(self.runtime.learning.task)

    async def test_disabled_entry_with_no_choices_reports_disabled_not_missing_selection(self):
        flow=self.flow(mesh_enabled=False)
        self.runtime.enabled.clear()
        result=await flow.async_step_mesh_learn({'targets':[]})
        self.assertEqual(result['errors']['base'],'mesh_not_ready')

    async def test_selected_target_learning_is_query_only_and_has_result(self):
        flow = self.flow()
        result = await flow.async_step_mesh_learn({'targets':[self.b.mac]})
        self.assertEqual(result['type'],'progress')
        await self.runtime.learning.task
        self.assertEqual(len(self.runtime.learning.results),1)
        self.assertEqual(self.runtime.learning.results[0].route,self.route)
        self.assertEqual(self.radios[1].writes,[])
        self.assertIsNone(self.b.last_mesh_gateway)
        result = await flow.async_step_mesh_progress()
        self.assertEqual(result['next_step_id'],'mesh_result')
        self.assertNotIn(flow._mesh_progress_changed,self.runtime.learning.listeners)
        result = await flow.async_step_mesh_result()
        self.assertIn('bestätigt',result['description_placeholders']['result'])

    async def test_second_flow_does_not_replace_active_run(self):
        first,second = self.flow(),self.flow()
        await self.prepare()
        self.radios[1].silent=True
        await first.async_step_mesh_learn({'targets':[self.b.mac]})
        task=self.runtime.learning.task
        result=await second.async_step_mesh_learn({'targets':[self.a.mac]})
        self.assertEqual(result['errors']['base'],'mesh_learning_running')
        self.assertIs(self.runtime.learning.task,task)
        await self.runtime.learning.stop()
        first.async_remove()

    async def test_closing_owner_cancels_and_releases_gateway(self):
        flow=self.flow()
        await self.prepare()
        self.radios[1].silent=True
        await flow.async_step_mesh_learn({'targets':[self.b.mac]})
        await asyncio.sleep(.001)
        flow.async_remove()
        await asyncio.gather(self.runtime.learning.task,return_exceptions=True)
        self.assertTrue(self.runtime.learning.cancelled)
        self.assertFalse(self.runtime.pool.users)
        self.assertEqual(self.a._status_pins,0)
        self.assertFalse(self.runtime.learning.listeners)

    async def test_closing_observer_does_not_cancel_owner(self):
        owner,observer=self.flow(),self.flow()
        await self.prepare()
        self.radios[1].silent=True
        await owner.async_step_mesh_learn({'targets':[self.b.mac]})
        await observer.async_step_mesh_progress()
        observer.async_remove()
        self.assertFalse(self.runtime.learning.task.cancelled())
        self.assertFalse(self.runtime.learning.cancelled)
        await self.runtime.learning.stop()
        owner.async_remove()

    async def test_cancel_confirmation_cannot_cancel_a_replacement_run(self):
        flow=self.flow()
        learner=self.runtime.learning
        async def blocked(route, **kwargs):await asyncio.Event().wait()
        with patch.object(learner,'probe',blocked):
            learner.start({self.b.mac})
            await asyncio.sleep(0)
            await flow.async_step_mesh_cancel()
            await learner.stop()
            learner.start({self.b.mac})
            result=await flow.async_step_mesh_cancel({})
            self.assertEqual(result['errors']['base'],'mesh_learning_changed')
            self.assertFalse(learner.cancelled)
            self.assertFalse(learner.task.done())
            await learner.stop()

    async def test_progress_updates_only_changed_activity(self):
        flow=self.flow()
        flow.cur_step={'step_id':'mesh_progress'}
        flow._mesh_progress_source=self.runtime.learning
        flow._mesh_report=self.runtime.learning.report
        self.runtime.learning.current_route=self.route
        flow._mesh_progress_changed()
        flow._mesh_progress_changed()
        self.assertEqual(flow.notified,1)
        self.assertIn('0x0001 → 0x0002',flow.cur_step['description_placeholders']['status'])

    async def test_ha_closing_observer_cancels_waiter_but_not_shared_learning(self):
        # HA FlowManager cancels progress_task BEFORE calling async_remove().
        # Contract: homeassistant/data_entry_flow.py, 2026.10.0.
        owner,observer=self.flow(),self.flow()
        await self.prepare()
        self.radios[1].silent=True
        await owner.async_step_mesh_learn({'targets':[self.b.mac]})
        progress=await observer.async_step_mesh_observe()
        shared=self.runtime.learning.task
        self.assertIsNot(progress['progress_task'],shared)
        await asyncio.sleep(0)
        progress['progress_task'].cancel()
        observer.async_remove()
        await asyncio.gather(progress['progress_task'],return_exceptions=True)
        self.assertFalse(shared.done())
        self.assertFalse(self.runtime.learning.cancelled)
        owner.async_remove()
        await asyncio.gather(shared,return_exceptions=True)

    async def test_ha_closing_owner_cancels_run_and_cleans_reservations(self):
        flow=self.flow()
        await self.prepare()
        self.radios[1].silent=True
        progress=await flow.async_step_mesh_learn({'targets':[self.b.mac]})
        await asyncio.sleep(.001)
        progress['progress_task'].cancel()
        flow.async_remove()
        await asyncio.gather(progress['progress_task'],self.runtime.learning.task,return_exceptions=True)
        self.assertTrue(self.runtime.learning.cancelled)
        self.assertFalse(self.runtime.pool.users)
        self.assertEqual(self.a._status_pins,0)

    async def test_actual_learning_cancel_finishes_dialog_waiter(self):
        flow=self.flow()
        await self.prepare()
        self.radios[1].silent=True
        progress=await flow.async_step_mesh_learn({'targets':[self.b.mac]})
        await asyncio.sleep(0)
        await self.runtime.learning.stop()
        await progress['progress_task']
        self.assertEqual((await flow.async_step_mesh_progress())['next_step_id'],'mesh_cancelled')
        self.assertTrue(flow._mesh_report['cancelled'])

    async def test_old_owner_can_observe_a_new_run_without_acquiring_ownership(self):
        flow=self.flow()
        learner=self.runtime.learning
        await flow.async_step_mesh_learn({'targets':[self.b.mac]})
        await learner.task
        await flow.async_step_mesh_progress()
        first_report=flow._mesh_report
        async def blocked(route, **kwargs):await asyncio.Event().wait()
        with patch.object(learner,'probe',blocked):
            learner.start({self.b.mac})
            await flow.async_step_mesh_observe()
            self.assertIs(flow._mesh_report,learner.report)
            self.assertIsNot(flow._mesh_report,first_report)
            self.assertFalse(flow._mesh_owns_learning)
            flow.async_remove()
            self.assertFalse(learner.cancelled)
            await learner.stop()

    async def test_old_progress_does_not_show_another_runs_activity(self):
        flow=self.flow()
        await flow.async_step_mesh_learn({'targets':[self.b.mac]})
        await self.runtime.learning.task
        old_task=flow._mesh_task
        before=flow.notified
        async def blocked(route, **kwargs):await asyncio.Event().wait()
        with patch.object(self.runtime.learning,'probe',blocked):
            self.runtime.learning.start({self.b.mac})
            await asyncio.sleep(0)
            flow._mesh_progress_changed()
            self.assertEqual(flow.notified,before)
            self.assertIs(flow._mesh_task,old_task)
            await self.runtime.learning.stop()
        flow.async_remove()

    async def test_eager_new_run_cannot_publish_progress_to_previous_dialog(self):
        flow=self.flow()
        learner=self.runtime.learning
        await flow.async_step_mesh_learn({'targets':[self.b.mac]})
        await learner.task
        before=flow.notified
        async def instant(route, **kwargs):return LearningResult(route,True,1,.001)
        loop=asyncio.get_running_loop()
        previous=loop.get_task_factory()
        try:
            loop.set_task_factory(asyncio.eager_task_factory)
            with patch.object(learner,'probe',instant):
                learner.start({self.b.mac})
                await learner.task
        finally:
            loop.set_task_factory(previous)
        self.assertEqual(flow.notified,before)
        flow.async_remove()

    async def test_partial_results_survive_cancel_and_storage_reload(self):
        self.b._availability.advertisement_time=mesh.time.monotonic()
        entered=asyncio.Event()
        calls=[]
        async def probe(route, **kwargs):
            calls.append(route)
            if len(calls)==1:return LearningResult(route,True,1,.01)
            entered.set()
            await asyncio.Event().wait()
        learner=self.runtime.learning
        with patch.object(learner,'probe',probe):
            learner.start()
            await entered.wait()
            await learner.stop()
        self.assertEqual(len(learner.results),1)
        other=MeshRuntime(self.hass,'network')
        await other.storage.load()
        self.assertEqual(len(other.state.routes),1)
        self.assertEqual(len(other.state.learning_results),1)
        self.assertTrue(learner.report['cancelled'])

    async def test_cancel_during_final_save_keeps_report_and_releases_reservation(self):
        learner=self.runtime.learning
        entered=asyncio.Event()
        async def save():
            entered.set()
            await asyncio.Event().wait()
        with patch.object(self.runtime.storage,'flush',save):
            learner.start({self.b.mac})
            await entered.wait()
            await learner.stop()
        self.assertEqual(len(learner.report['results']),1)
        self.assertTrue(learner.report['cancelled'])
        self.assertTrue(learner.report['save_error'])
        self.assertFalse(learner.report['running'])
        self.assertFalse(self.runtime.pool.users)

    async def test_result_snapshot_survives_a_new_run(self):
        flow=self.flow()
        await flow.async_step_mesh_learn({'targets':[self.b.mac]})
        await self.runtime.learning.task
        report=flow._mesh_report
        before=copy.deepcopy(report)
        await flow.async_step_mesh_progress()
        self.runtime.learning.start({self.a.mac})
        await self.runtime.learning.task
        self.assertEqual(report,before)
        self.assertIsNot(report,self.runtime.learning.report)

    async def test_storage_error_is_visible_in_learning_result(self):
        flow=self.flow()
        with patch.object(self.runtime.storage,'flush',AsyncMock(side_effect=OSError('disk'))):
            with self.assertLogs('awox112.mesh.learning',level='ERROR'):
                await flow.async_step_mesh_learn({'targets':[self.b.mac]})
                await self.runtime.learning.task
        result=await flow.async_step_mesh_result()
        self.assertIn('nicht dauerhaft gespeichert',result['description_placeholders']['result'])
        self.assertIn((self.a.mac,self.b.mac),self.runtime.state.routes)
        flow.async_remove()

    async def test_preferred_learned_gateway_precedes_visible_direct(self):
        self.learn()
        self.b._availability.advertisement_time=mesh.time.monotonic()
        self.rule(preferred=self.a.mac)
        self.assertEqual(self.runtime.routing.candidates(self.b.mac)[0],self.route)
        await self.command()
        self.assertEqual(self.b.last_mesh_gateway,1)

    async def test_blocked_direct_and_mesh_have_no_hidden_fallback(self):
        self.learn()
        self.rule(blocked=(self.a.mac,self.b.mac))
        self.assertEqual(self.runtime.routing.candidates(self.b.mac),[])
        with self.assertRaises(mesh.fx.CommandExecutionFailed):await self.command()
        self.assertFalse(self.radios)

    async def test_stale_known_preference_is_attemptable_without_claiming_reachability(self):
        self.learn()
        self.b._availability.advertisement_time=mesh.time.monotonic()
        self.rule(preferred=self.a.mac)
        self.a._availability.advertisement_time=None
        self.assertEqual(self.runtime.routing.candidates(self.b.mac)[0].gateway,self.a.mac)
        self.assertFalse(self.runtime.routing.mesh_routes(self.b.mac))

    async def test_preferences_do_not_survive_target_identity_change(self):
        self.rule(blocked=(self.a.mac,self.b.mac))
        self.b.configured_mesh_id=9
        self.assertEqual(self.runtime.routing.preference(self.b.mac),{})
        self.assertEqual(self.runtime.routing.candidates(self.b.mac)[0].target_id,9)

    async def test_disabling_mesh_restores_direct_control_but_preserves_rules(self):
        self.learn()
        self.rule(preferred=self.a.mac,blocked=(self.b.mac,))
        saved=copy.deepcopy(self.runtime.state.preferences)
        self.runtime.enabled.discard(self.b.mac)
        candidates=self.runtime.routing.candidates(self.b.mac)
        self.assertEqual([r.gateway for r in candidates],[self.b.mac])
        await self.command()
        self.assertIn(2,self.radios)
        self.assertEqual(self.runtime.state.preferences,saved)
        self.runtime.enabled.add(self.b.mac)
        self.assertFalse(self.runtime.routing.permitted(self.b.mac,self.b.mac))

    async def test_gateway_rule_does_not_apply_to_reused_destination(self):
        self.rule(blocked=(self.a.mac,))
        self.assertFalse(self.runtime.routing.permitted(self.a.mac,self.b.mac))
        self.a.configured_mesh_id=3
        self.assertTrue(self.runtime.routing.permitted(self.a.mac,self.b.mac))

    async def test_gateway_preferences_are_persisted_and_identity_bound(self):
        self.learn()
        self.rule(preferred=self.a.mac,blocked=(self.b.mac,))
        await self.runtime.storage.flush()
        other=MeshRuntime(self.hass,'network')
        await other.storage.load()
        self.assertEqual(other.state.preferences,self.runtime.state.preferences)

    async def test_preferences_store_only_referenced_identities_and_skip_noop_notifications(self):
        changes=[]
        self.runtime.state.listeners.add(lambda:changes.append(True))
        self.rule(preferred=self.a.mac)
        self.assertEqual(self.runtime.state.preferences[self.b.mac]['gateway_ids'],{self.a.mac:1})
        self.rule(preferred=self.a.mac)
        self.assertEqual(len(changes),1)
        self.rule()
        self.assertNotIn(self.b.mac,self.runtime.state.preferences)
        self.rule()
        self.assertEqual(len(changes),2)

    async def test_storage_discards_empty_rules_and_unrelated_gateway_ids(self):
        key=self.runtime.storage.store.key
        mesh.MemoryStore.data[key]=dict(preferences={self.b.mac:dict(
            target_id=2,preferred=None,blocked=[],gateway_ids={self.a.mac:1,self.b.mac:2})},
            last_gateways={'bad':1,self.b.mac:1})
        await self.runtime.storage.load()
        self.assertFalse(self.runtime.state.preferences)
        self.assertEqual(self.runtime.state.last_gateways,{self.b.mac:1})
        mesh.MemoryStore.data[key]['preferences'][self.b.mac]['preferred']=self.a.mac
        await self.runtime.storage.load()
        self.assertEqual(self.runtime.state.preferences[self.b.mac]['gateway_ids'],{self.a.mac:1})

    async def test_ui_rejects_preferred_blocked_and_unknown_gateways(self):
        flow=self.flow()
        flow._mesh_target=self.b.mac
        self.learn()
        result=await flow.async_step_mesh_lamp({'preferred':self.a.mac,'blocked':[self.a.mac]})
        self.assertEqual(result['errors']['base'],'mesh_preferred_blocked')
        result=await flow.async_step_mesh_lamp({'preferred':'bad','blocked':[]})
        self.assertEqual(result['errors']['base'],'mesh_invalid_gateway')
        self.assertFalse(self.runtime.state.preferences)

    async def test_ui_saved_preference_is_used_by_actual_control(self):
        flow=self.flow()
        flow._mesh_target=self.b.mac
        self.learn()
        result=await flow.async_step_mesh_lamp({'preferred':self.a.mac,'blocked':[self.b.mac]})
        self.assertEqual(result['step_id'],'mesh')
        await self.command()
        self.assertEqual(self.b.last_mesh_gateway,1)

    async def test_save_failure_reports_in_memory_rule(self):
        flow=self.flow()
        flow._mesh_target=self.b.mac
        self.learn()
        with patch.object(self.runtime.storage,'flush',AsyncMock(side_effect=OSError('disk'))):
            result=await flow.async_step_mesh_lamp({'preferred':'auto','blocked':[self.a.mac]})
        self.assertEqual(result['errors']['base'],'mesh_save_failed')
        self.assertFalse(self.runtime.routing.permitted(self.a.mac,self.b.mac))

    async def test_target_unloaded_during_failed_rule_save_returns_to_selection(self):
        flow=self.flow()
        flow._mesh_target=self.b.mac
        self.learn()
        async def disappearing():
            self.runtime.clients.pop(self.b.mac)
            raise OSError('disk')
        with patch.object(self.runtime.storage,'flush',disappearing):
            result=await flow.async_step_mesh_lamp({'preferred':'auto','blocked':[]})
        self.assertEqual(result['step_id'],'mesh_lamps')

    async def test_entry_unloaded_during_failed_rule_save_aborts_cleanly(self):
        flow=self.flow()
        flow._mesh_target=self.b.mac
        self.learn()
        async def disappearing():
            self.hass.data.pop(DATA_MESH)
            raise OSError('disk')
        with patch.object(self.runtime.storage,'flush',disappearing):
            result=await flow.async_step_mesh_lamp({'preferred':'auto','blocked':[]})
        self.assertEqual(result['reason'],'mesh_not_ready')

    async def _assert_retirement_blocks_new_leases(self, retiring):
        runtime=self.runtime
        await runtime.remove('entry')
        await runtime.add('gateway',[(self.a,{})],True)
        await runtime.add('target',[(self.b,{})],True)
        self.learn()
        held,cleaning,release=asyncio.Event(),asyncio.Event(),asyncio.Event()
        async def holding():
            async with runtime.pool.reserve(self.a.mac,self.b.mac):
                held.set()
                try:await asyncio.Event().wait()
                except asyncio.CancelledError:
                    cleaning.set()
                    await release.wait()
        holder=asyncio.create_task(holding())
        await held.wait()
        removal=asyncio.create_task(runtime.remove(retiring))
        try:
            await asyncio.wait_for(cleaning.wait(),1)
            result=await asyncio.wait_for(runtime.learning.probe(self.route),.1)
            self.assertFalse(result.success)
            self.assertEqual(result.polls,0)
            self.assertFalse(self.radios)
            self.assertEqual(set(runtime.pool.users),{holder})
        finally:
            release.set()
            await removal
        self.assertFalse(runtime.pool.users)
        self.assertEqual(self.a._status_pins,0)

    async def test_gateway_retirement_rejects_new_users_during_cleanup(self):
        await self._assert_retirement_blocks_new_leases('gateway')

    async def test_target_retirement_rejects_new_users_during_cleanup(self):
        await self._assert_retirement_blocks_new_leases('target')

    async def test_blocking_active_route_stops_remaining_writes_without_reroute(self):
        self.learn()
        await self.prepare()
        self.radios[1].hook=lambda mid:self.rule(blocked=(self.a.mac,))
        with self.assertRaises(mesh.fx.CommandExecutionFailed):
            await self.command(mesh.fx.T(color=(20,70),brightness=220))
        self.assertEqual(len(self.radios[1].writes),1)
        self.assertNotIn(2,self.radios)

    async def test_blocking_direct_path_checks_actual_write_boundary(self):
        await self.prepare()
        self.radios[1].hook=lambda mid:self.runtime.state.set_preferences(
            self.a.mac,1,None,[self.a.mac],{self.a.mac:1})
        with self.assertRaises(mesh.fx.CommandExecutionFailed):
            await self.command(mesh.fx.T(color=(20,70),brightness=220),client=self.a)
        self.assertEqual(len(self.radios[1].writes),1)

    async def test_blocking_queued_direct_request_prevents_connection(self):
        entered,release=asyncio.Event(),asyncio.Event()
        async def holder():
            async with self.runtime.pool.reserve(self.a.mac,self.a.mac):
                entered.set()
                await release.wait()
        holding=asyncio.create_task(holder())
        await entered.wait()
        request=asyncio.create_task(self.command(client=self.a))
        await asyncio.sleep(.001)
        self.runtime.state.set_preferences(self.a.mac,1,None,[self.a.mac],{self.a.mac:1})
        release.set()
        await holding
        with self.assertRaises(mesh.fx.CommandExecutionFailed):await request
        self.assertFalse(self.radios)

    async def test_blocking_queued_learning_prevents_connection(self):
        entered,release=asyncio.Event(),asyncio.Event()
        async def holder():
            async with self.runtime.pool.reserve(self.a.mac,self.a.mac):
                entered.set()
                await release.wait()
        holding=asyncio.create_task(holder())
        await entered.wait()
        learning=asyncio.create_task(self.runtime.learning.probe(self.route))
        await asyncio.sleep(.001)
        self.rule(blocked=(self.a.mac,))
        release.set()
        await holding
        result=await learning
        self.assertFalse(result.success)
        self.assertEqual(result.polls,0)
        self.assertFalse(self.radios)

    async def test_preferred_connection_failure_falls_back_before_control(self):
        third=self.client(3,visible=True)
        await self.runtime.add('third',[(third,{})],True)
        self.learn()
        self.learn(Route(third.mac,self.b.mac,3,2))
        self.rule(preferred=self.a.mac)
        with patch.object(self.a,'async_send_selected_once',AsyncMock(side_effect=ConnectionError('connect'))):
            await self.command()
        self.assertEqual(self.b.last_mesh_gateway,3)
        self.assertNotIn(1,self.radios)
        self.assertEqual(self.runtime.state.metrics[self.a.mac]['failed'],1)

    async def test_learning_respects_blocked_gateway_rules(self):
        self.rule(blocked=(self.a.mac,))
        await self.runtime.learning.run({self.b.mac})
        self.assertEqual(self.runtime.learning.total,0)
        self.assertFalse(self.radios)

    async def test_learning_evaluates_gateway_reachability_once_per_run_snapshot(self):
        third=self.client(3,visible=True)
        self.b._availability.advertisement_time=mesh.time.monotonic()
        await self.runtime.add('third',[(third,{})],True)
        self.runtime.state.listeners.clear()
        calls=[]
        usable=self.runtime.routing.usable
        def track(client):
            # Count snapshot selection, not reachability publications caused by
            # the now deliberate connection/disconnection measurements.
            if self.runtime.learning.total == 0:
                calls.append(client.mac)
            return usable(client)
        async def result(route, **kwargs):return LearningResult(route,True,1,.01)
        with patch.object(self.runtime.routing,'usable',track),patch.object(self.runtime.learning,'probe',result):
            await self.runtime.learning.run()
        self.assertEqual(sorted(calls),sorted(self.runtime.clients))
        self.assertEqual(self.runtime.learning.total,30)

    async def test_candidate_policy_matrix(self):
        third=self.client(3,visible=True)
        await self.runtime.add('third',[(third,{})],True)
        clients=[self.a,self.b,third]
        ids={c.mac:c.configured_mesh_id for c in clients}
        self.runtime.state.listeners.clear()
        for a in clients:
            for b in clients:
                if a is not b:self.learn(Route(a.mac,b.mac,ids[a.mac],ids[b.mac]))
        cases=0
        for enabled in range(8):
            self.runtime.enabled.clear()
            self.runtime.enabled.update(c.mac for i,c in enumerate(clients) if enabled & 1<<i)
            for visible in range(8):
                for i,c in enumerate(clients):
                    c._availability.advertisement_time=mesh.time.monotonic() if visible & 1<<i else None
                for blocked in range(8):
                    blocked_macs=[c.mac for i,c in enumerate(clients) if blocked & 1<<i]
                    for preferred in [None,*ids]:
                        # Test selection invariants even for a conflicting persisted choice.
                        self.runtime.state.preferences[self.b.mac]=dict(
                            target_id=2,preferred=preferred,blocked=blocked_macs,gateway_ids=ids)
                        candidates=self.runtime.routing.candidates(self.b.mac)
                        gateways=[r.gateway for r in candidates]
                        if self.b.mac not in self.runtime.enabled:
                            self.assertEqual(gateways,[self.b.mac])
                        else:
                            self.assertFalse(set(gateways)&set(blocked_macs))
                            for route in candidates:
                                if route.gateway!=route.target:
                                    self.assertIn(route.gateway,self.runtime.enabled)
                                    self.assertTrue(self.runtime.routing.allowed(route))
                            if preferred in gateways:self.assertEqual(gateways[0],preferred)
                        self.assertEqual(len(gateways),len(set(gateways)))
                        cases+=1
        self.assertEqual(cases,2048)

    async def test_storage_json_shape_matrix_never_raises(self):
        from dataclasses import asdict
        values=[None,True,False,-1,0,1,'bad',self.a.mac,[],{},[self.a.mac],{'x':1},
                float('nan'),float('inf'),10**400]
        samples={
            'preferences':dict(target_id=2,preferred=self.a.mac,blocked=[],gateway_ids={self.a.mac:1}),
            'metrics':dict(mesh_id=1,attempts=1,successful=1,failed=0,cancelled=0,
                           total_ms=5,last_ms=5,min_ms=5,max_ms=5,rssi=None),
            'learning_results':dict(route=asdict(self.route),success=True,polls=1,elapsed=.01,error=None)}
        key=self.runtime.storage.store.key
        cases=0
        for section,sample in samples.items():
            for field in sample:
                for value in values:
                    item={**sample,field:value}
                    data={section:[item] if section=='learning_results' else {
                        self.b.mac if section=='preferences' else self.a.mac:item}}
                    mesh.MemoryStore.data[key]=data
                    await self.runtime.storage.load()
                    cases+=1
        self.assertEqual(cases,285)

    async def test_progress_listener_failure_does_not_break_learning(self):
        def broken():raise RuntimeError('closed observer')
        self.runtime.learning.listeners.add(broken)
        with self.assertLogs('awox112.mesh.learning',level='ERROR'):
            await self.runtime.learning.run({self.b.mac})
        self.assertTrue(self.runtime.learning.results[0].success)
        self.assertFalse(self.radios[1].writes)
        self.runtime.learning.listeners.clear()

    async def test_new_connections_measured_once_warm_preparation_not_counted(self):
        await self.runtime.pool.prepare(self.a)
        metric=copy.deepcopy(self.runtime.state.metrics[self.a.mac])
        await self.runtime.pool.prepare(self.a)
        self.assertEqual(self.runtime.state.metrics[self.a.mac],metric)
        self.assertEqual(metric['attempts'],1)
        self.assertEqual(metric['successful'],1)
        self.assertFalse(self.radios[1].writes)

    async def test_failed_and_cancelled_preparation_are_distinct_metrics(self):
        for error,field in ((ConnectionError('connect'),'failed'),(asyncio.CancelledError(),'cancelled')):
            with patch.object(self.a,'async_send_selected_once',AsyncMock(side_effect=error)):
                with self.assertRaises(type(error)):await self.runtime.pool.prepare(self.a)
            self.assertEqual(self.runtime.state.metrics[self.a.mac][field],1)
        self.assertEqual(self.runtime.state.metrics[self.a.mac]['attempts'],2)

    async def test_reset_metrics_preserves_routes_rules_results_and_history(self):
        flow=self.flow()
        self.learn()
        self.rule(preferred=self.a.mac)
        await self.command()
        metric_keys = {'metrics', 'ble_metrics', 'ble_metrics_started_at'}
        expected={k:copy.deepcopy(v) for k,v in self.runtime.state.export().items() if k not in metric_keys}
        self.assertTrue(self.runtime.state.metrics)
        await flow.async_step_mesh_metrics_reset({})
        self.assertFalse(self.runtime.state.metrics)
        self.assertEqual({k:v for k,v in self.runtime.state.export().items() if k not in metric_keys},expected)

    async def test_reset_metrics_save_error_is_visible(self):
        flow=self.flow()
        with patch.object(self.runtime.storage,'flush',AsyncMock(side_effect=OSError('disk'))):
            result=await flow.async_step_mesh_metrics_reset({})
        self.assertEqual(result['errors']['base'],'mesh_save_failed')

    async def test_matrix_shows_direct_learned_failed_and_blocked_paths(self):
        flow=self.flow()
        self.learn()
        self.runtime.state.note_direct(self.a.mac, 1, 'confirmed')
        self.runtime.state.learned(LearningResult(Route(self.b.mac,self.a.mac,2,1),False,7,1.75,'No valid target reply'))
        result=await flow.async_step_mesh_matrix()
        text=result['description_placeholders']['matrix']
        for value in ('BLE','✓','–'):self.assertIn(value,text)
        self.rule(blocked=(self.a.mac,))
        result=await flow.async_step_mesh_matrix()
        self.assertEqual(text,result['description_placeholders']['matrix'])
        self.assertFalse(self.runtime.routing.permitted(self.a.mac,self.b.mac))

    async def test_oversight_escapes_names_in_markdown(self):
        flow=self.flow()
        self.runtime.devices[self.a.mac]={'name':'A | <script>\nsecond'}
        flow._mesh_target = self.a.mac
        result=await flow.async_step_mesh_lamp()
        text=result['description_placeholders']['details']
        self.assertNotIn('<script>',text)
        self.assertIn('\\|',text)

    async def test_malformed_optional_storage_sections_are_ignored(self):
        for value in (None,[],42,'invalid'):
            mesh.MemoryStore.data[self.runtime.storage.store.key] = dict(
                preferences=value,metrics=value,learning_results=value)
            await self.runtime.storage.load()
        mesh.MemoryStore.data[self.runtime.storage.store.key] = dict(
            preferences={self.b.mac:dict(target_id=2,preferred=self.a.mac,blocked=[self.a.mac],gateway_ids={self.a.mac:1})},
            metrics={self.a.mac:dict(mesh_id=1,attempts=True)},
            learning_results=[{},None,dict(route={},success=True,polls=1,elapsed=float('nan'))])
        await self.runtime.storage.load()
        self.assertFalse(self.runtime.state.preferences)
        self.assertFalse(self.runtime.state.metrics)
        self.assertFalse(self.runtime.state.learning_results)

    async def test_metrics_survive_reload(self):
        await self.runtime.pool.prepare(self.a)
        await self.runtime.storage.flush()
        other=MeshRuntime(self.hass,'network')
        await other.storage.load()
        self.assertEqual(other.state.metrics,self.runtime.state.metrics)

    def test_translations_have_matching_options_and_no_test_switching_instructions(self):
        base=Path(__file__).resolve().parents[1]/'custom_components/awox_connect_z/translations'
        texts=[json.loads((base/(language+'.json')).read_text(encoding='utf-8'))['options'] for language in ('de','en')]
        self.assertEqual(set(texts[0]['step']),set(texts[1]['step']))
        for text in texts:
            self.assertNotIn('mesh_learn',text['step']['runtime']['data'])
            for step in text['step'].values():
                for target in step.get('menu_options',{}):self.assertIn(target,text['step'])


if __name__=='__main__':unittest.main()
