"""Presentation and transition contracts for the v46 UI adapted to 1911.

HA/BLE are simulated. This is not a visual acceptance test in live HA.
"""
import asyncio
import copy
import json
from pathlib import Path
import types
import unittest
from unittest.mock import AsyncMock, patch

import test_mesh_ui as ui
mesh = ui.mesh
fx = mesh.fx
cf = mesh.opt.config_flow
from awox112.mesh import MeshRuntime
from awox112.mesh.models import Route, LearningResult


class RestorationTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = mesh.MeshTests.asyncSetUp
    asyncTearDown = mesh.MeshTests.asyncTearDown
    client = mesh.MeshTests.client
    learn = mesh.MeshTests.learn
    executor = mesh.MeshTests.executor
    command = mesh.MeshTests.command
    prepare = mesh.MeshTests.prepare
    flow = ui.ManagementTests.flow

    async def form(self, flow, step, user_input=None):
        with patch.object(cf.vol, 'Required', lambda key, **kw: (key, repr(kw))), \
             patch.object(cf, 'SelectSelector', lambda config: config):
            return await getattr(flow, 'async_step_' + step)(user_input)

    async def test_idle_menu_exact_order_and_no_extra_settings_page(self):
        flow = self.flow()
        self.assertEqual((await flow.async_step_init())['menu_options'], ['runtime', 'mesh'])
        self.assertEqual((await flow.async_step_mesh())['menu_options'],
                         ['mesh_overview', 'mesh_learn', 'mesh_lamps', 'mesh_matrix', 'mesh_metrics', 'init'])
        self.assertFalse(self.radios)

    async def test_open_learning_during_active_run_shows_error_before_submit(self):
        first, second = self.flow(), self.flow()
        blocked = asyncio.Event()
        async def probe(route, **kwargs):await blocked.wait()
        with patch.object(self.runtime.learning, 'probe', probe):
            await first.async_step_mesh_learn({'targets': [self.b.mac]})
            original = self.runtime.learning.task
            for _ in range(3):
                result = await second.async_step_mesh_learn()
                self.assertEqual(result['errors'], {'base': 'mesh_learning_running'})
                self.assertIs(self.runtime.learning.task, original)
            await self.runtime.learning.stop()
        first.async_remove()
        self.assertFalse((await second.async_step_mesh_learn())['errors'])

    async def test_native_progress_translation_renders_the_activity_placeholder(self):
        flow = self.flow()
        learning = self.runtime.learning
        learning.current_route = self.route
        learning.activity = 'connecting_gateway'
        root=Path(__file__).resolve().parents[1]/'custom_components/awox_connect_z/translations'
        for language in ('de','en'):
            self.hass.config.language=language
            options=json.loads((root/(language+'.json')).read_text(encoding='utf-8'))['options']
            template=options['progress']['mesh_learning']
            self.assertIn('\n\n{status}',template)
            rendered=template.format(status=flow._mesh_progress_text())
            self.assertIn('0x0001',rendered)
            self.assertIn('Verbinde mit Gateway' if language=='de' else 'Connecting to gateway',rendered)

    async def test_real_learning_publishes_connection_query_and_saving_activities(self):
        stages=[]
        learner=self.runtime.learning
        learner.listeners.add(lambda:stages.append(learner.activity))
        await self.runtime.learning.run({self.b.mac})
        self.assertIn('waiting_gateway',stages)
        self.assertIn('connecting_gateway',stages)
        self.assertIn('testing_route',stages)
        self.assertIn('saving',stages)
        self.assertEqual(stages[-1],'done')
        self.assertTrue(learner.results[0].success)
        self.assertFalse(self.radios[1].writes)

    async def test_changed_activity_refreshes_existing_dialog(self):
        flow=self.flow()
        learner=self.runtime.learning
        flow.cur_step={'step_id':'mesh_progress'}
        flow._mesh_progress_source=learner
        flow._mesh_report=learner.report
        learner.current_route=self.route
        for index,activity in enumerate(('connecting_gateway','testing_route','saving'),1):
            learner.activity=activity
            flow._mesh_progress_changed()
            self.assertEqual(flow.notified,index)
            flow._mesh_progress_changed()
            self.assertEqual(flow.notified,index)
        self.assertIn('Speichere Ergebnis',flow.cur_step['description_placeholders']['status'])

    async def test_list_selectors_labels_defaults_and_field_order(self):
        flow = self.flow()
        self.runtime.devices[self.a.mac] = {'name': 'Lampe A'}
        self.runtime.devices[self.b.mac] = {'name': 'Lampe B'}
        result = await self.form(flow, 'mesh_learn')
        key, selector = next(iter(result['data_schema'].items()))
        self.assertIn(self.a.mac, key[1]); self.assertIn(self.b.mac, key[1])
        self.assertEqual(selector['mode'], 'list'); self.assertTrue(selector['multiple'])
        self.assertEqual([o['label'] for o in selector['options']], ['Lampe A · 0x0001', 'Lampe B · 0x0002'])
        lamps = await self.form(flow, 'mesh_lamps')
        self.assertEqual(next(iter(lamps['data_schema'].values()))['mode'], 'list')
        await self.form(flow, 'mesh_lamps', {'target': self.b.mac})
        self.learn()
        page = await self.form(flow, 'mesh_lamp')
        self.assertEqual([k[0] for k in page['data_schema']], ['preferred', 'blocked'])
        preferred, blocked = page['data_schema'].values()
        self.assertEqual(preferred['mode'], 'list'); self.assertEqual(blocked['mode'], 'list')
        self.assertTrue(blocked['multiple'])
        self.assertEqual([o['label'] for o in preferred['options']],
                         ['Automatisch auswählen', '0x0001', '0x0002 · Diese Lampe – direkte BLE-Verbindung'])

    async def test_target_selection_is_entry_scoped_but_gateways_can_be_shared(self):
        flow = self.flow()
        third = self.client(3, visible=True)
        await self.runtime.add('other', [(third, {'name': 'Other'})], True)
        page = await self.form(flow, 'mesh_lamps')
        self.assertEqual(len(next(iter(page['data_schema'].values()))['options']), 2)
        bad = await self.form(flow, 'mesh_lamps', {'target': third.mac})
        self.assertEqual(bad['errors']['base'], 'mesh_invalid_target')
        learned = Route(third.mac, self.b.mac, 3, 2)
        self.learn(learned)
        flow._mesh_target = self.b.mac
        page = await self.form(flow, 'mesh_lamp')
        self.assertIn(third.mac, [o['value'] for o in next(iter(page['data_schema'].values()))['options']])

    async def test_gateway_page_identity_rules_table_and_save_return(self):
        flow = self.flow()
        self.runtime.devices[self.b.mac] = {'name': 'Target'}
        self.learn()
        flow._mesh_target = self.b.mac
        page = await flow.async_step_mesh_lamp()
        detail = page['description_placeholders']['details']
        for text in ('**Target**', 'Mesh-ID: `0x0002`', f'MAC: `{self.b.mac}`',
                     'Bevorzugte Gateway-Lampe: **Automatisch**', 'Gesperrt: —',
                     '**Zulässige Gateways**', '| Gateway | Nachweis |', '1911-Zielantwort bestätigt'):
            self.assertIn(text, detail)
        result = await flow.async_step_mesh_lamp({'preferred': self.a.mac, 'blocked': [self.b.mac]})
        self.assertEqual(result['step_id'], 'mesh')
        detail = (await flow.async_step_mesh_lamp())['description_placeholders']['details']
        self.assertIn('**`0x0001`**', detail); self.assertIn('Gesperrt: `0x0002`', detail)

    async def test_back_transitions_and_result_menu(self):
        flow = self.flow()
        for step in ('mesh_overview', 'mesh_matrix', 'mesh_result_back', 'mesh_cancelled'):
            self.assertEqual((await getattr(flow, 'async_step_'+step)({}))['step_id'], 'mesh')
        flow._mesh_report = {'targets': [self.a.mac, self.b.mac], 'results': []}
        result = await flow.async_step_mesh_result()
        self.assertEqual(result['type'], 'menu')
        self.assertEqual(result['menu_options'], ['mesh_result_back'])
        self.assertIn('2 nicht geprüft', result['description_placeholders']['result'])

    async def test_overview_direct_is_separate_and_matrix_has_no_invented_ble(self):
        flow = self.flow()
        text = (await flow.async_step_mesh_overview())['description_placeholders']['overview']
        self.assertIn('2 Lampen · 0 bestätigt · 0 nicht bestätigt · 2 nicht geprüft · 0 Fehler', text)
        self.assertIn('| Lampe | Direkt-BLE | Mesh-Status | Bestätigte Gateways |', text)
        matrix = (await flow.async_step_mesh_matrix())['description_placeholders']['matrix']
        self.assertIn('| **0x0001** | ? | ? |', matrix)
        self.runtime.state.note_direct(self.a.mac, 1, 'confirmed')
        text = flow._mesh_summary()
        self.assertIn('| `0x0001` | ✓ | Nicht geprüft | — |', text)
        self.assertIn('0 bestätigt', text)
        matrix = (await flow.async_step_mesh_matrix())['description_placeholders']['matrix']
        self.assertIn('| Gateway ↓ / Ziel → |', matrix)
        self.assertIn('| **0x0001** | BLE | ? |', matrix)

    async def test_historical_proof_is_visible_but_cannot_control(self):
        flow = self.flow()
        self.learn()
        self.runtime.state.learned(LearningResult(self.route, False, 7, 1.75, 'No valid target reply'))
        self.assertEqual(flow._mesh_route_mark(self.a.mac,self.b.mac), '✓*')
        self.assertFalse(self.runtime.routing.allowed(self.route))
        self.assertNotIn(self.route, self.runtime.routing.candidates(self.b.mac))
        self.assertIn('Bestätigt*', flow._mesh_summary())
        self.b.configured_mesh_id = 9
        self.assertEqual(flow._mesh_route_mark(self.a.mac,self.b.mac), '?')

    async def test_result_remains_bound_to_finished_run(self):
        flow = self.flow()
        await flow.async_step_mesh_learn({'targets': [self.b.mac]})
        await self.runtime.learning.task
        first = (await flow.async_step_mesh_result())['description_placeholders']['result']
        self.runtime.learning.start({self.a.mac})
        await self.runtime.learning.task
        self.assertEqual((await flow.async_step_mesh_result())['description_placeholders']['result'], first)

    async def test_empty_metrics_still_lists_lamps_and_has_reset_back(self):
        flow = self.flow()
        result = await flow.async_step_mesh_metrics()
        text = result['description_placeholders']['metrics']
        self.assertIn('| `0x0001` | — | — | — |', text)
        self.assertIn('| `0x0002` | — | — | — |', text)
        self.assertIn('**Messwerte seit:** —', text)
        self.assertIn('keine zusätzlichen Testverbindungen', text)
        self.assertEqual(result['menu_options'], ['mesh_metrics_reset','mesh'])
        self.assertFalse(self.radios)

    async def test_pure_ble_metrics_history_bounded_reload_and_reset(self):
        flow = self.flow()
        self.learn()
        for index in range(60):
            self.runtime._note_ble(self.a, 'successful', index, -60)
        self.runtime._note_ble(self.a, 'failed', 999, -61)
        metric = self.runtime.state.ble_metrics[self.a.mac]
        self.assertEqual(metric['durations_ms'], list(range(10,60)))
        self.assertEqual(len(metric['recent_attempts']), 50)
        text = (await flow.async_step_mesh_metrics())['description_placeholders']['metrics']
        self.assertIn('| `0x0001` | 34 ms | 60/61 | -61 dBm |', text)
        self.assertNotIn('999 ms', text)
        await self.runtime.storage.flush()
        other = MeshRuntime(self.hass, 'network')
        await other.storage.load()
        self.assertEqual(other.state.ble_metrics, self.runtime.state.ble_metrics)
        self.assertEqual(other.state.direct, self.runtime.state.direct)
        self.assertEqual(other.state.route_history, self.runtime.state.route_history)
        direct, history = copy.deepcopy(self.runtime.state.direct), copy.deepcopy(self.runtime.state.route_history)
        result = await flow.async_step_mesh_metrics_reset({})
        self.assertEqual(result['step_id'], 'mesh_metrics')
        self.assertEqual(self.runtime.state.ble_metrics,{})
        self.assertEqual(self.runtime.state.direct,direct)
        self.assertEqual(self.runtime.state.route_history,history)
        self.assertIsNotNone(self.runtime.state.ble_metrics_started_at)

    async def test_ble_quality_ranking_minimum_samples_and_preference(self):
        third = self.client(3, visible=True)
        await self.runtime.add('other', [(third, {})], True)
        self.learn(); self.learn(Route(third.mac, self.b.mac,3,2))
        for _ in range(5):
            self.runtime._note_ble(self.a, 'successful', 200, -50)
            self.runtime._note_ble(third, 'successful', 50, -60)
        self.assertEqual(self.runtime.routing.candidates(self.b.mac)[0].gateway,third.mac)
        for _ in range(3):self.runtime._note_ble(third,'failed',1,-60)
        self.assertEqual(self.runtime.routing.candidates(self.b.mac)[0].gateway,self.a.mac)
        self.runtime.state.set_preferences(self.b.mac,2,third.mac,[],{third.mac:3})
        self.assertEqual(self.runtime.routing.candidates(self.b.mac)[0].gateway,third.mac)
        self.b._availability.advertisement_time = fx.time.monotonic()
        self.runtime.state.preferences.clear()
        self.assertEqual(self.runtime.routing.candidates(self.b.mac)[0].gateway,self.a.mac)

    async def test_at_most_one_alternative_before_writing(self):
        third, fourth = self.client(3, visible=True), self.client(4, visible=True)
        await self.runtime.add('other', [(third, {}),(fourth,{})], True)
        for gateway in (self.a,third,fourth):self.learn(Route(gateway.mac,self.b.mac,gateway.configured_mesh_id,2))
        with patch.object(self.runtime.pool,'prepare',AsyncMock(side_effect=ConnectionError('connect'))) as prepare:
            with self.assertRaises(fx.CommandExecutionFailed):await self.command()
        self.assertEqual(prepare.await_count,2)
        self.assertFalse(self.radios)

    async def test_corrupt_new_storage_sections_never_raise(self):
        for value in (None,False,0,'bad',[],{},float('inf'),10**400):
            mesh.MemoryStore.data[self.runtime.storage.store.key] = dict(
                direct={self.a.mac:value}, route_history=[value],
                ble_metrics={self.a.mac:value},ble_metrics_started_at=value)
            await self.runtime.storage.load()
        self.assertFalse(self.runtime.state.ble_metrics)
        self.assertFalse(self.runtime.state.direct)
        self.assertFalse(self.runtime.state.route_history)

    def test_translations_keep_old_menu_descriptions_and_field_subtitles(self):
        root=Path(__file__).resolve().parents[1]/'custom_components/awox_connect_z/translations'
        for language in ('de','en'):
            options=json.loads((root/(language+'.json')).read_text(encoding='utf-8'))['options']
            steps=options['step']
            for page,fields in (('init',('runtime','mesh')),('mesh',('mesh_overview','mesh_learn','mesh_lamps','mesh_matrix','mesh_metrics')),
                                ('mesh_metrics',('mesh_metrics_reset',))):
                for field in fields:self.assertTrue(steps[page]['menu_option_descriptions'][field])
            for page, fields in (('mesh_learn',('targets',)), ('mesh_lamps',('target',)), ('mesh_lamp',('preferred','blocked'))):
                for field in fields:self.assertTrue(steps[page]['data_description'][field])
            self.assertIn('1911',steps['mesh_learn']['description'])
            self.assertIn('{details}',steps['mesh_lamp']['description'])
            self.assertEqual(steps['mesh_result']['menu_options'].keys(),{'mesh_result_back'})


class BLEMeasurementTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_connect_boundary_excludes_authentication_and_is_passive(self):
        client = fx.AwoxConnectZClient(fx.Hass(),fx.MAC,'mesh','password',default_transition=0,
            idle_disconnect=10,configured_mesh_id=fx.MID,
            command_semaphore=asyncio.Semaphore(2),max_concurrent_commands=2)
        events=[]
        class BLE:
            def __init__(self,*args,**kwargs):self.is_connected=False
            async def connect(self,**kwargs):
                events.append('connect'); self.is_connected=True
            async def disconnect(self):self.is_connected=False
        client._async_find_device_runtime=lambda:object()
        client._actual_connection_source=lambda ble:None
        async def auth():
            events.append('auth'); client._session_key=fx.KEY; client._session_generation+=1
        client._async_authenticate=auth
        client._ble_connection_observer=lambda *args:events.append(('measurement',args))
        with patch('awox112.client.BleakClientWithServiceCache',BLE):
            await client._async_ensure_connected(fast_runtime=True)
            await client._async_ensure_connected(fast_runtime=True)
        self.assertEqual([e if isinstance(e,str) else e[0] for e in events],['connect','measurement','auth'])
        self.assertEqual(events[1][1][0],'successful')
        self.assertGreaterEqual(events[1][1][1],0)
        await client.async_close()

    async def test_measurement_callback_exception_cannot_break_success(self):
        client = fx.AwoxConnectZClient(fx.Hass(),fx.MAC,'mesh','password',default_transition=0,
            idle_disconnect=10,configured_mesh_id=fx.MID,
            command_semaphore=asyncio.Semaphore(2),max_concurrent_commands=2)
        client._ble_connection_observer=lambda *args:(_ for _ in ()).throw(ValueError('test'))
        with self.assertLogs('awox112.client',level='ERROR'):
            client._note_ble_connection('successful',fx.time.monotonic())
        await client.async_close()


if __name__ == '__main__':unittest.main(verbosity=2)
