"""Mesh contract tests using production clients, planner, crypto and confirmation.

The BLE backend and HA storage are simulated. These tests do not claim radio
reliability or physical lamp behavior.
"""
import asyncio
import copy
import time
import types
import unittest
from unittest.mock import patch, AsyncMock

import test_optimization as opt
fx = opt.fx
from awox112.mesh import MeshRuntime, async_attach, async_detach, network_key, DATA_MESH
from awox112.mesh.models import Route, LearningResult
from awox112.mesh import learning as lm
from awox112.mesh import dispatch as dm
from awox112.status_1911 import Status1911Store
from awox112.availability import AvailabilityState
from awox112.const import STATUS_CHAR_UUID


class MemoryStore:
    data = {}

    def __init__(self, hass, version, key):
        self.key = key

    async def async_load(self):
        return copy.deepcopy(self.data.get(self.key))

    async def async_save(self, data):
        self.data[self.key] = copy.deepcopy(data)

    def async_delay_save(self, builder, delay):
        self.data[self.key] = copy.deepcopy(builder())


fx.module('homeassistant.helpers.storage', Store=MemoryStore)


class Radio:
    def __init__(self, owner, lamps):
        self.owner, self.lamps = owner, lamps
        self.is_connected = True
        self.notify = None
        self.writes = []
        self.polls = []
        self.silent = False
        self.passive = True
        self.fail_write = False
        self.delay = 0
        self.hook = None

    async def disconnect(self):
        self.is_connected = False

    async def start_notify(self, char, callback):
        self.notify = callback

    async def write_gatt_char(self, char, packet, response):
        assert response is False
        if char == STATUS_CHAR_UUID:
            assert packet == b'\x01'
            return
        frame = fx.p.aes_decrypt_reversed(fx.KEY, packet[1:])
        if packet[0] == 2:
            mid = int.from_bytes(frame[3:5], 'little')
            self.polls.append(mid)
            if not self.silent:
                self.emit(mid)
            return
        mid = int.from_bytes(frame[2:4], 'big')
        self.writes.append((mid, frame, time.monotonic()))
        if self.fail_write:
            raise ConnectionError('uncertain radio write')
        state = self.lamps[mid][1]
        if frame[5] == 6:
            state['power'] = bool(frame[7])
            if not state['power']:
                state['effect'] = None
        elif frame[5] == 8 and frame[7] == 4:
            state.update(power=True, brightness=frame[8])
        elif frame[5] == 8 and frame[7] == 0x10:
            state['effect'] = 'Candle' if frame[8] else None
        elif frame[7] == 6:
            state.update(color_mode=True, hue=frame[8], saturation=frame[9], effect=None)
        elif frame[7] == 0x0A:
            state.update(color_mode=False, mired=int.from_bytes(frame[8:10], 'little'), effect=None)
        elif frame[7] == 0x41:
            state['effect'] = 'Color cycle'
        elif frame[7] == 0x47:
            state['effect'] = None
        if self.hook:
            self.hook(mid)
        await asyncio.sleep(self.delay)
        if not self.silent and self.passive:
            self.emit(mid)

    def emit(self, mid, *, outer=None, generation=None):
        mac, state = self.lamps[mid]
        outer = outer or (0x80 if mid == self.owner.configured_mesh_id else 0x82)
        self.notify(None, fx.payload(state, mid=mid, mac=mac, outer_type=outer))


class MeshTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        MemoryStore.data = {}
        self.hass = fx.Hass()
        self.hass.data = {}
        self.hass.async_create_background_task = lambda coro, *args: asyncio.create_task(coro)
        self.lamps = {}
        self.clients = []
        self.radios = {}
        self.patches = [patch.object(fx.cc, 'CONFIRMATION_AUTO_WAIT_MS', 2),
                        patch.object(fx.cc, 'CONFIRMATION_POLL_WINDOW_MS', 2),
                        patch.object(lm, 'CONFIRMATION_POLL_WINDOW_MS', 2),
                        patch.object(dm, 'CONFIRMATION_POLL_WINDOW_MS', 2)]
        for item in self.patches:
            item.start()
        self.runtime = MeshRuntime(self.hass, 'network')
        self.a = self.client(1, visible=True)
        self.b = self.client(2)
        await self.runtime.add('entry', [(c, {}) for c in self.clients], True)
        self.route = Route(self.a.mac, self.b.mac, 1, 2)

    async def asyncTearDown(self):
        for entry in tuple(self.runtime.entries):
            await self.runtime.remove(entry)
        for client in self.clients:
            await client.async_close()
        for item in reversed(self.patches):
            item.stop()

    def client(self, mid, visible=False):
        mac = f'02:00:00:00:00:{mid:02X}'
        c = fx.AwoxConnectZClient(self.hass, mac, 'name', 'secret', default_transition=0,
                                 idle_disconnect=300, configured_mesh_id=mid,
                                 command_semaphore=asyncio.Semaphore(2), max_concurrent_commands=2)
        c._latest_service_info = lambda: None
        if visible:
            c._availability.advertisement_time = time.monotonic()
        self.lamps[mid] = (mac, dict(power=True, color_mode=False, brightness=127,
                                    effect=None, mired=250, hue=0, saturation=64))
        async def connect(**kwargs):
            if c.connected:
                return
            c._session_generation += 1
            c._session_key = fx.KEY
            c._client = self.radios[mid] = Radio(c, self.lamps)
            c._status_ready_generation = None
        c._async_ensure_connected = connect
        self.clients.append(c)
        return c

    def learn(self, route=None):
        self.runtime.state.learned(LearningResult(route or self.route, True, 1, .01))

    def executor(self, client=None, on=True):
        client = client or self.b
        manager = fx.CommandManager(fx.S(True, on, None), minimum_interval_ms=0)
        executor = fx.CommandExecutor(client.mesh_dispatch, manager, mesh_id=client.configured_mesh_id)
        return manager, executor

    async def command(self, target=None, client=None):
        manager, executor = self.executor(client)
        manager.submit(target or fx.T(power=False))
        await executor.async_execute_group()
        return manager, executor

    async def prepare(self, c=None):
        c = c or self.a
        await c.async_send_selected_once(lambda: None)

    async def test_learning_uses_only_target_polls_and_keeps_history_empty(self):
        result = await self.runtime.learning.probe(self.route)
        self.assertTrue(result.success)
        self.assertEqual(result.polls, 1)
        self.assertEqual(self.radios[1].writes, [])
        self.assertEqual(self.radios[1].polls, [2])
        self.runtime.state.learned(result)
        self.assertEqual(self.b.bluetooth_status, 'mesh_available')
        self.assertIsNone(self.b.last_mesh_gateway)
        self.assertIsNone(self.b.last_seen)
        self.assertIsNone(self.b.signal_strength)
        self.assertIsNone(self.b.last_connection_time)

    async def test_learning_is_directed(self):
        self.learn()
        self.assertEqual(len(self.runtime.state.routes), 1)
        self.assertEqual(self.runtime.routing.mesh_routes(self.a.mac), [])

    async def test_target_control_uses_gateway_and_existing_confirmation(self):
        self.learn()
        _, e = await self.command(fx.T(color=(120, 80), brightness=222, effect='Candle'))
        self.assertEqual(e.last_confirmed_state.outer_type, 0x82)
        self.assertEqual(e.last_confirmed_state.mesh_id, 2)
        self.assertEqual(self.b.last_mesh_gateway, 1)
        self.assertEqual(self.b.bluetooth_status, 'mesh_connected')
        self.assertFalse(self.b.connected)
        self.assertTrue(self.b.available)
        self.assertTrue(all(mid == 2 for mid, _, _ in self.radios[1].writes))
        self.assertEqual(self.b.mesh_dispatch.confirmation.diagnostics['confirmed'], 1)

    async def test_gateway_connection_alone_does_not_confirm_target(self):
        self.learn()
        await self.prepare()
        self.assertEqual(self.b.bluetooth_status, 'mesh_available')

    async def test_initial_status_does_not_record_control_success(self):
        self.learn()
        state = await self.b.mesh_dispatch.async_initial_status()
        self.assertEqual(state.mesh_id, 2)
        self.assertIsNone(self.b.last_mesh_gateway)
        self.assertEqual(self.b.bluetooth_status, 'mesh_available')

    async def test_disconnect_and_expiry_use_gateway_evidence(self):
        self.learn()
        await self.command()
        await self.a._async_disconnect(mark_liveness=True)
        self.assertEqual(self.b.bluetooth_status, 'mesh_available')
        self.assertEqual(self.b.last_mesh_gateway, 1)
        self.a._availability.advertisement_time = 0
        self.a._availability.connection_time = 0
        self.runtime.refresh()
        self.assertEqual(self.b.bluetooth_status, 'unreachable')
        self.assertFalse(self.b.available)

    async def test_new_direct_evidence_does_not_displace_ready_gateway(self):
        self.learn()
        await self.command()
        now = time.monotonic()
        self.b._availability.connection_time = now
        self.b._availability.advertisement_time = now - 1
        self.assertEqual(self.b.bluetooth_status, 'recently_connected')
        self.b._availability.advertisement_time = now + .01
        self.assertEqual(self.b.bluetooth_status, 'visible')
        await self.command(fx.T(brightness=180), self.b)
        self.assertNotIn(2, self.radios)
        self.assertEqual(self.b.bluetooth_status, 'visible')
        self.assertEqual(self.runtime.routing.status(self.b.mac), 'mesh_connected')
        self.assertEqual(self.b.last_mesh_gateway, 1)

    async def test_mesh_unavailable_cannot_recursively_use_another_mesh_gateway(self):
        self.learn()
        c = self.client(3)
        await self.runtime.add('third', [(c, {})], True)
        self.learn(Route(self.b.mac, c.mac, 2, 3))
        self.assertEqual(self.b.bluetooth_status, 'mesh_available')
        self.assertEqual(c.bluetooth_status, 'unreachable')

    async def test_disabled_target_and_gateway_are_not_routable(self):
        self.learn()
        for client in (self.a, self.b):
            self.runtime.enabled.remove(client.mac)
            self.assertFalse(self.runtime.routing.mesh_routes(self.b.mac))
            self.runtime.enabled.add(client.mac)

    async def test_changed_target_identity_invalidates_route(self):
        self.learn()
        self.b.configured_mesh_id = 9
        self.assertFalse(self.runtime.routing.mesh_routes(self.b.mac))

    async def test_preparation_failure_falls_back_before_selection(self):
        self.learn()
        c = self.client(3, visible=True)
        await self.runtime.add('third', [(c, {})], True)
        self.learn(Route(c.mac, self.b.mac, 3, 2))
        self.a._async_ensure_connected = AsyncMock(side_effect=ConnectionError('no connection'))
        await self.command()
        self.a._async_ensure_connected.assert_awaited_once()
        self.assertNotIn(1, self.radios)
        self.assertEqual(self.b.last_mesh_gateway, 3)

    async def test_uncertain_write_never_reroutes(self):
        self.learn()
        c = self.client(3, visible=True)
        await self.runtime.add('third', [(c, {})], True)
        self.learn(Route(c.mac, self.b.mac, 3, 2))
        await self.prepare()
        self.radios[1].fail_write = True
        with self.assertRaises(fx.CommandExecutionFailed):
            await self.command()
        self.assertNotIn(3, self.radios)
        self.assertIsNone(self.b.last_mesh_gateway)
        self.assertFalse(self.runtime.pool.users)

    async def test_unconfirmed_group_has_three_then_seven_polls_and_no_reroute(self):
        self.learn()
        await self.prepare()
        self.radios[1].silent = True
        with self.assertRaises(fx.CommandExecutionFailed):
            await self.command()
        self.assertEqual(len(self.radios[1].writes), 2)
        self.assertEqual(len(self.radios[1].polls), 10)
        self.assertIsNone(self.b.last_mesh_gateway)

    async def test_manual_poll_confirms_correct_target(self):
        self.learn()
        await self.prepare()
        self.radios[1].passive = False
        await self.command()
        self.assertEqual(self.radios[1].polls, [2])
        self.assertEqual(self.b.last_mesh_gateway, 1)

    async def test_same_gateway_different_targets_can_interleave(self):
        self.learn()
        await self.prepare()
        self.radios[1].delay = .002
        await asyncio.gather(self.command(fx.T(color=(20, 80), brightness=150), self.b),
                             self.command(fx.T(color=(40, 60), brightness=100), self.a))
        destinations = [mid for mid, _, _ in self.radios[1].writes]
        changes = sum(a != b for a, b in zip(destinations, destinations[1:]))
        self.assertGreater(changes, 1)
        for target in (1, 2):
            frames = [frame for mid, frame, _ in self.radios[1].writes if mid == target]
            self.assertEqual([frame[5] for frame in frames], [0, 8, 6])
            self.assertEqual([frame[7] for frame in frames], [6, 4, 1])
        self.assertFalse(self.runtime.pool.users)

    async def test_different_gateways_are_independent(self):
        entered = asyncio.Event()
        release = asyncio.Event()
        async def hold():
            async with self.runtime.pool.reserve(self.a.mac, self.a.mac):
                entered.set()
                await release.wait()
        task = asyncio.create_task(hold())
        await entered.wait()
        async with self.runtime.pool.reserve(self.b.mac, self.b.mac):
            self.assertFalse(task.done())
        release.set()
        await task

    async def test_cancel_queued_user_keeps_current_gateway_connection(self):
        entered = asyncio.Event()
        release = asyncio.Event()
        async def hold():
            async with self.runtime.pool.reserve(self.a.mac, self.a.mac):
                await self.prepare()
                entered.set()
                await release.wait()
        async def queued():
            async with self.runtime.pool.reserve(self.a.mac, self.b.mac):
                self.fail('cancelled queued user must not enter')
        task = asyncio.create_task(hold())
        await entered.wait()
        waiting = asyncio.create_task(queued())
        await asyncio.sleep(0)
        waiting.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiting
        self.assertTrue(self.a.connected)
        self.assertEqual(self.a._status_pins, 1)
        release.set()
        await task
        self.assertEqual(self.a._status_pins, 0)

    async def test_session_loss_between_writes_does_not_reconnect(self):
        self.learn()
        await self.prepare()
        self.radios[1].hook = lambda mid: setattr(self.radios[1], 'is_connected', False)
        with self.assertRaises(fx.CommandExecutionFailed):
            await self.command(fx.T(color=(80, 60), brightness=200))
        self.assertEqual(len(self.radios[1].writes), 1)
        self.assertEqual(self.a._session_generation, 1)

    async def test_route_revoked_at_actual_write_boundary(self):
        self.learn()
        endpoint = self.b.mesh_dispatch
        async with endpoint.status_transaction():
            self.runtime.state.routes.clear()
            with self.assertRaises(ConnectionError):
                await endpoint.async_send_selected_once(lambda: fx.client.RuntimeSelectedCommand())
        self.assertFalse(self.radios[1].writes)

    async def test_noop_request_does_not_create_gateway_history(self):
        self.learn()
        await self.command(fx.T(power=True))
        self.assertIsNone(self.b.last_mesh_gateway)
        self.assertFalse(self.radios)

    async def test_learning_failure_revokes_only_tested_direction(self):
        self.learn()
        reverse = Route(self.b.mac, self.a.mac, 2, 1)
        self.learn(reverse)
        self.runtime.state.learned(LearningResult(self.route, False, 7, 1.75, 'no response'))
        self.assertNotIn((self.a.mac, self.b.mac), self.runtime.state.routes)
        self.assertIn((self.b.mac, self.a.mac), self.runtime.state.routes)

    async def test_old_learning_frame_cannot_satisfy_new_probe(self):
        await self.prepare()
        store = self.a.status_for_target(self.b.mac, 2)
        self.radios[1].emit(2)
        self.radios[1].silent = True
        result = await self.runtime.learning.probe(self.route)
        self.assertFalse(result.success)
        self.assertEqual(result.polls, 7)

    async def test_stores_separate_outer_type_identity_and_generation(self):
        direct = Status1911Store(self.a.mac, 1)
        mesh = Status1911Store(self.b.mac, 2, outer_type=0x82)
        direct.begin(7)
        mesh.begin(7)
        cases = [(self.b.mac, 2, 0x82, 7, True),
                 (self.b.mac, 2, 0x80, 7, False),
                 (self.a.mac, 2, 0x82, 7, False),
                 (self.b.mac, 1, 0x82, 7, False),
                 (self.b.mac, 2, 0x82, 6, False)]
        for mac, mid, outer, gen, expected in cases:
            mesh.frames.clear()
            packet = fx.payload({}, mid=mid, mac=mac, outer_type=outer)
            mesh.feed(packet, fx.KEY, gen)
            direct.feed(packet, fx.KEY, gen)
            self.assertEqual(bool(mesh.frames), expected)
            self.assertFalse(direct.frames)

    async def test_reauthentication_ends_target_stores(self):
        await self.prepare()
        store = self.a.status_for_target(self.b.mac, 2)
        self.radios[1].emit(2)
        old_callback = self.radios[1].notify
        await self.a._async_disconnect()
        self.assertFalse(store.active)
        await self.prepare()
        old_callback(None, fx.payload({}, mid=2, mac=self.b.mac, outer_type=0x82))
        self.assertFalse(store.frames)
        self.radios[1].emit(2)
        self.assertEqual(store.latest.session_generation, 2)

    async def test_same_local_generation_on_different_gateways_isolated(self):
        await self.prepare(self.a)
        await self.prepare(self.b)
        store_a = self.a.status_for_target(self.b.mac, 2)
        store_b = self.b.status_for_target(self.a.mac, 1)
        self.radios[1].emit(2)
        self.assertTrue(store_a.frames)
        self.assertFalse(store_b.frames)

    async def test_storage_restores_routes_and_history_but_never_active_proof(self):
        self.learn()
        await self.command()
        await self.runtime.storage.flush()
        other = MeshRuntime(self.hass, 'network')
        await other.storage.load()
        self.assertEqual(other.state.routes, self.runtime.state.routes)
        self.assertEqual(other.state.last_gateways, {self.b.mac: 1})
        self.assertFalse(other.state.proofs)
        self.assertEqual(other.routing.mesh_routes(self.b.mac), [])

    async def test_storage_ignores_malformed_routes(self):
        MemoryStore.data[self.runtime.storage.store.key] = {
            'routes': [None, {}, {'gateway': 'bad', 'target': 'bad', 'gateway_id': 1, 'target_id': 2},
                       {'gateway': self.a.mac, 'target': self.b.mac, 'gateway_id': True, 'target_id': 2}],
            'last_gateways': {'bad': -1, self.b.mac: 1},
        }
        await self.runtime.storage.load()
        self.assertFalse(self.runtime.state.routes)
        self.assertEqual(self.runtime.state.last_gateways, {self.b.mac: 1})

    async def test_unload_gateway_clears_proofs_for_other_entry(self):
        await self.runtime.remove('entry')
        await self.runtime.add('gateway', [(self.a, {})], True)
        await self.runtime.add('target', [(self.b, {})], True)
        self.learn()
        await self.command()
        await self.runtime.remove('gateway')
        self.assertFalse(self.runtime.state.proofs)
        self.assertEqual(self.b.last_mesh_gateway, 1)
        self.assertFalse(self.b.available)

    async def test_unload_cancels_pending_learning_and_releases_pins(self):
        await self.prepare()
        self.radios[1].silent = True
        self.runtime.learning.start()
        await asyncio.sleep(.001)
        await self.runtime.remove('entry')
        self.assertTrue(self.runtime.learning.task.done())
        self.assertFalse(self.runtime.pool.users)
        self.assertEqual(self.a._status_pins, 0)

    async def test_credentials_isolate_runtime_and_storage(self):
        self.assertNotEqual(network_key('ab', 'c'), network_key('a', 'bc'))
        self.assertNotEqual(network_key('mesh', 'a'), network_key('mesh', 'b'))
        self.assertNotIn('secret', network_key('mesh', 'secret'))

    async def test_availability_combination_matrix(self):
        for direct in (None, 'unreachable', 'connected', 'visible', 'recently_connected'):
            for mesh in (None, 'unreachable', 'mesh_connected', 'mesh_available'):
                expected = direct if direct in ('connected', 'visible', 'recently_connected') else mesh or direct
                self.assertEqual(AvailabilityState.combine_status(direct, mesh), expected)

    async def test_duplicate_registration_rejected_without_modifying_existing(self):
        with self.assertRaises(ValueError):
            await self.runtime.add('duplicate', [(self.b, {})], True)
        self.assertNotIn('duplicate', self.runtime.entries)
        self.assertEqual(len(self.runtime.clients), 2)

    async def test_full_target_matrix_uses_real_sequence_and_status_pipeline(self):
        self.learn()
        requests = [fx.T(power=False), fx.T(brightness=255), fx.T(color=(359, 100)),
                    fx.T(cct=2700), fx.T(effect='Candle'), fx.T(effect='Color cycle'),
                    fx.T(stop_effect=True), fx.T(color=(180, 40), brightness=80),
                    fx.T(cct=6500, brightness=200, effect='Candle')]
        for target in requests:
            # Every case starts in a known ON/no-effect state, independently.
            self.lamps[2][1].update(power=True, effect=None)
            _, executor = await self.command(target)
            if executor.last_confirmed_state is not None:
                self.assertEqual(executor.last_confirmed_state.mesh_id, 2)
        self.assertFalse(self.runtime.pool.users)

    async def test_target_command_diagnostics_are_not_only_on_gateway(self):
        self.learn()
        await self.command()
        self.assertEqual(self.b.last_command, 'power:off')
        data = opt.diagnostics.build_client_diagnostics(self.b)
        self.assertEqual(data['bluetooth_status'], 'mesh_connected')
        self.assertTrue(data['available'])
        self.assertEqual(data['command_confirmation']['confirmed'], 1)
        self.assertNotIn('secret', str(data))

    async def test_target_failure_diagnostics_are_retained(self):
        self.learn()
        await self.prepare()
        self.radios[1].fail_write = True
        with self.assertRaises(fx.CommandExecutionFailed):
            await self.command()
        self.assertEqual(self.b.last_error_category, 'mesh_transport')

    async def test_gateway_store_removed_with_target(self):
        await self.runtime.remove('entry')
        await self.runtime.add('gateway', [(self.a, {})], True)
        await self.runtime.add('target', [(self.b, {})], True)
        await self.prepare()
        store = self.a.status_for_target(self.b.mac, 2)
        await self.runtime.remove('target')
        self.assertFalse(store.active)
        self.assertFalse(self.a._mesh_status_stores)
        self.assertTrue(self.a.connected)

    async def test_store_failure_cannot_block_unload_cleanup(self):
        with patch.object(self.runtime.storage, 'flush', AsyncMock(side_effect=OSError('disk'))):
            with self.assertLogs('awox112.mesh', level='ERROR'):
                await self.runtime.remove('entry')
        self.assertFalse(self.runtime.clients)
        self.assertFalse(self.runtime.pool.users)
        self.assertIsNone(self.a.mesh_dispatch)

    async def test_duplicate_destination_is_rejected(self):
        c = self.client(3)
        c.configured_mesh_id = 1
        with self.assertRaisesRegex(ValueError, 'Duplicate lamp mesh'):
            await self.runtime.add('third', [(c, {})], True)
        self.assertNotIn('third', self.runtime.entries)

    async def test_registry_shares_only_identical_credentials_and_detaches(self):
        entry = types.SimpleNamespace(entry_id='shared', options={},
                                      data={'mesh_name': 'name', 'mesh_password': 'secret'})
        runtime = await async_attach(self.hass, entry, [(self.a, {})])
        second = types.SimpleNamespace(entry_id='shared2', options={}, data=entry.data)
        self.assertIs(await async_attach(self.hass, second, [(self.b, {})]), runtime)
        await async_detach(self.hass, 'shared')
        self.assertIn(DATA_MESH, self.hass.data)
        await async_detach(self.hass, 'shared2')
        self.assertNotIn(DATA_MESH, self.hass.data)

    async def test_partial_registration_failure_cleans_its_clients(self):
        entry = types.SimpleNamespace(entry_id='partial', options={},
                                      data={'mesh_name': 'other', 'mesh_password': 'credentials'})
        c = self.client(3)
        d = self.client(4)
        c.async_add_diagnostic_listener = lambda callback: (_ for _ in ()).throw(RuntimeError('subscribe'))
        with self.assertRaisesRegex(RuntimeError, 'subscribe'):
            await async_attach(self.hass, entry, [(c, {}), (d, {})])
        self.assertFalse(self.hass.data[DATA_MESH])
        self.assertIsNone(c.mesh_dispatch)

    async def test_refresh_publishes_same_status_to_light_and_sensor(self):
        self.learn()
        observed = []
        self.b.async_add_diagnostic_listener(lambda: observed.append((self.b.available, self.b.bluetooth_status)))
        await self.command()
        self.assertTrue(observed)
        for available, status in observed:
            self.assertEqual(available, AvailabilityState.is_available(status))

    async def test_history_sensor_only_changes_after_control(self):
        description = next(d for d in opt.sensor.SENSOR_DESCRIPTIONS if d.key == 'last_mesh_gateway')
        sensor = opt.sensor.AwoxConnectZDiagnosticSensor(self.b, {'mesh_id': 2}, description)
        self.assertIsNone(sensor.native_value)
        self.learn()
        await self.b.mesh_dispatch.async_initial_status()
        self.assertIsNone(sensor.native_value)
        await self.command()
        self.assertEqual(sensor.native_value, '0x0001')
        await self.a._async_disconnect()
        self.assertEqual(sensor.native_value, '0x0001')

    async def test_options_learning_progress_preserves_selected_settings(self):
        self.hass.data[DATA_MESH] = {'network': self.runtime}
        flow = opt.config_flow.AwoxConnectZOptionsFlow()
        flow.hass = self.hass
        flow.config_entry = types.SimpleNamespace(entry_id='entry', options={'default_transition': .7})
        flow.async_create_entry = lambda **kw: dict(type='create_entry', **kw)
        flow.async_show_progress = lambda **kw: dict(type='progress', **kw)
        flow.async_show_progress_done = lambda **kw: dict(type='progress_done', **kw)
        result = await flow.async_step_mesh_learn({'targets': [self.b.mac]})
        self.assertEqual(result['step_id'], 'mesh_progress')
        await self.runtime.learning.task
        result = await flow.async_step_mesh_progress()
        self.assertEqual(result['next_step_id'], 'mesh_result')
        self.assertEqual(flow.config_entry.options, {'default_transition': .7})

    async def test_learning_unloaded_or_disabled_entry_reports_error(self):
        flow = opt.config_flow.AwoxConnectZOptionsFlow()
        flow.hass = self.hass
        flow.config_entry = types.SimpleNamespace(entry_id='entry', options={})
        result = await flow.async_step_mesh_learn({'targets': [self.b.mac]})
        self.assertEqual(result['reason'], 'mesh_not_ready')

    async def test_attach_after_identity_change_prunes_saved_routes(self):
        self.learn()
        await self.runtime.storage.flush()
        await self.runtime.remove('entry')
        self.b.configured_mesh_id = 9
        await self.runtime.add('entry', [(self.a, {}), (self.b, {})], True)
        self.assertFalse(self.runtime.state.routes)

    async def test_reloaded_gateway_cannot_resurrect_same_generation_proof(self):
        await self.runtime.remove('entry')
        await self.runtime.add('gateway', [(self.a, {})], True)
        await self.runtime.add('target', [(self.b, {})], True)
        self.learn()
        await self.command()
        await self.runtime.remove('gateway')
        # Deliberately reuse the still connected test client/generation, which
        # is stronger than a real reload (that closes the connection).
        await self.runtime.add('gateway', [(self.a, {})], True)
        self.assertEqual(self.b.bluetooth_status, 'mesh_available')

    async def test_full_integration_setup_unload_attaches_mesh_before_entities(self):
        hass = fx.Hass()
        hass.data = {}
        hass.loop = asyncio.get_running_loop()
        hass.async_create_background_task = lambda coro, *a: asyncio.create_task(coro)
        checks = []
        async def forward(entry, platforms):
            clients = hass.data['awox_connect_z'][entry.entry_id]
            checks.append(all(c.mesh_dispatch is not None for c, _ in clients))
        hass.config_entries = types.SimpleNamespace(async_forward_entry_setups=forward,
                                                     async_unload_platforms=AsyncMock(return_value=True))
        cleanups = []
        entry = types.SimpleNamespace(entry_id='real', version=2, options={},
            data={'setup_method': 'local', 'mesh_name': 'test', 'mesh_password': 'secret',
                  'devices': [{'mac': self.a.mac, 'mesh_id': 1}, {'mac': self.b.mac, 'mesh_id': 2}]},
            async_on_unload=cleanups.append)
        with patch.object(opt.setup.bluetooth, 'async_register_advertisement_callback', lambda *a: lambda: None, create=True), \
             patch.object(opt.setup.bluetooth, 'async_register_callback', lambda *a, **kw: lambda: None, create=True), \
             patch.object(opt.setup.bluetooth, 'BluetoothScanningMode', types.SimpleNamespace(ACTIVE='active'), create=True):
            self.assertTrue(await opt.setup.async_setup_entry(hass, entry))
            clients = list(hass.data['awox_connect_z']['real'])
            self.assertEqual(checks, [True])
            self.assertTrue(await opt.setup.async_unload_entry(hass, entry))
        self.assertNotIn(DATA_MESH, hass.data)
        self.assertTrue(all(c.closed and c._availability_task is None for c, _ in clients))
        for cleanup in cleanups:
            cleanup()

    async def test_all_preparations_fail_without_consuming_control(self):
        self.learn()
        self.a._async_ensure_connected = AsyncMock(side_effect=ConnectionError('offline'))
        self.b._async_ensure_connected = AsyncMock(side_effect=ConnectionError('offline'))
        with self.assertRaises(Exception):
            await self.command()
        self.assertFalse(self.radios)
        self.assertFalse(self.runtime.pool.users)
        self.assertEqual(self.a._status_pins, 0)

    async def test_cancel_active_group_cleans_gateway_reservation(self):
        self.learn()
        await self.prepare()
        self.radios[1].delay = 10
        task = asyncio.create_task(self.command())
        await asyncio.sleep(.01)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertFalse(self.runtime.pool.users)
        self.assertEqual(self.a._status_pins, 0)
        self.assertIsNone(self.b.mesh_dispatch.route)

    async def test_corrupt_route_collection_is_ignored(self):
        for data in ({'routes': None}, {'routes': 42}, {'routes': {}}, [], 'bad'):
            MemoryStore.data[self.runtime.storage.store.key] = data
            await self.runtime.storage.load()
            self.assertFalse(self.runtime.state.routes)

    async def test_preparation_failure_preserves_newer_unselected_request(self):
        self.learn()
        manager, executor = self.executor()
        manager.submit(fx.T(power=False))
        old = manager.request_generation
        async def fail(**kwargs):
            manager.submit(fx.T(brightness=200))
            raise ConnectionError('cannot prepare')
        self.a._async_ensure_connected = fail
        self.b._async_ensure_connected = fail
        with self.assertRaises(fx.CommandExecutionFailed) as caught:
            await executor.async_execute_group()
        self.assertNotIn(manager.request_generation, caught.exception.failed_generations)
        self.assertIn(manager.request_generation, manager.pending_generations)
        self.assertNotEqual(old, manager.request_generation)

    async def test_direct_connection_failure_can_fall_back_to_learned_gateway(self):
        self.learn()
        self.b._availability.advertisement_time = time.monotonic()
        self.b._async_ensure_connected = AsyncMock(side_effect=ConnectionError('direct offline'))
        await self.command()
        self.b._async_ensure_connected.assert_awaited_once()
        self.assertEqual(self.b.last_mesh_gateway, 1)

    async def test_group_repeat_stays_on_same_gateway_and_reuses_exact_plaintexts(self):
        self.learn()
        await self.prepare()
        radio = self.radios[1]
        radio.silent = True
        radio.hook = lambda mid: setattr(radio, 'silent', False) if len(radio.writes) == 2 else None
        await self.command()
        self.assertEqual(len(radio.polls), 3)
        self.assertEqual(len(radio.writes), 2)
        self.assertEqual(radio.writes[0][1], radio.writes[1][1])
        self.assertEqual(self.b.mesh_dispatch.confirmation.diagnostics['repeats'], 1)
        self.assertEqual(self.b.last_mesh_gateway, 1)

    async def test_standard_fade_and_spacing_before_effect_are_preserved(self):
        self.learn()
        manager = fx.CommandManager(fx.S(True, True, None), minimum_interval_ms=250)
        executor = fx.CommandExecutor(self.b.mesh_dispatch, manager, mesh_id=2)
        manager.submit(fx.T(cct=6500, appearance_transition=.2, effect='Candle'))
        await executor.async_execute_group()
        writes = self.radios[1].writes
        self.assertEqual(len(writes), 3)
        self.assertGreaterEqual(writes[1][2] - writes[0][2], .49)
        self.assertGreaterEqual(writes[2][2] - writes[1][2], .24)

    async def test_direct_and_mesh_sequence_matrix_match_except_destination(self):
        self.learn()
        targets = [fx.T(power=False), fx.T(power=True), fx.T(brightness=180),
                   fx.T(color=(254, 99)), fx.T(cct=2700), fx.T(effect='Candle'),
                   fx.T(effect='Color cycle'), fx.T(stop_effect=True),
                   fx.T(cct=6500, brightness=210, effect='Candle')]
        compared = 0
        for on in (False, True):
            for effect in (None, 'Candle', 'Color cycle'):
                for request in targets:
                    batches = []
                    for client in (self.a, self.b):
                        self.lamps[client.configured_mesh_id][1].update(power=on, effect=effect)
                        manager, executor = self.executor(client, on)
                        manager.set_state(fx.S(True, on, effect))
                        manager.submit(request)
                        count = len(self.radios[1].writes) if 1 in self.radios else 0
                        await executor.async_execute_group()
                        frames = [frame for _, frame, _ in self.radios[1].writes[count:]] if 1 in self.radios else []
                        # CRC and destination differ; every command and payload
                        # byte must be identical for direct and mesh routes.
                        batches.append([frame[1:2] + frame[4:] for frame in frames])
                    self.assertEqual(*batches, msg=(on, effect, request))
                    compared += 1
        self.assertEqual(compared, 54)

    async def test_last_detach_and_concurrent_attach_keep_runtime_registered(self):
        data = {'mesh_name': 'concurrent', 'mesh_password': 'secret'}
        first = types.SimpleNamespace(entry_id='first', options={}, data=data)
        second = types.SimpleNamespace(entry_id='second', options={}, data=data)
        runtime = await async_attach(self.hass, first, [(self.a, {})])
        entered, release = asyncio.Event(), asyncio.Event()
        flush = runtime.storage.flush
        async def delayed_flush():
            entered.set()
            await release.wait()
            await flush()
        with patch.object(runtime.storage, 'flush', delayed_flush):
            detach = asyncio.create_task(async_detach(self.hass, 'first'))
            await entered.wait()
            attach = asyncio.create_task(async_attach(self.hass, second, [(self.b, {})]))
            await asyncio.sleep(0)
            self.assertFalse(attach.done())
            release.set()
            await detach
            current = await attach
        self.assertIs(self.hass.data[DATA_MESH][network_key('concurrent', 'secret')], current)
        self.assertIn('second', current.entries)
        await async_detach(self.hass, 'second')

    async def test_listener_failure_does_not_fail_confirmed_control(self):
        self.learn()
        observed = []
        def broken():
            raise OSError('storage unavailable')
        self.runtime.state.listeners.update((broken, lambda: observed.append(True)))
        with self.assertLogs('awox112.mesh.state', level='ERROR'):
            await self.command()
        self.assertTrue(observed)
        self.assertEqual(self.b.last_mesh_gateway, 1)
        self.assertEqual(self.b.mesh_dispatch.confirmation.diagnostics['confirmed'], 1)
        self.runtime.state.listeners.remove(broken)

    async def test_rejected_duplicate_mac_batch_does_not_leak_subscriptions(self):
        c = self.client(3)
        other = self.client(4)
        other.mac = c.mac
        with self.assertRaises(ValueError):
            await self.runtime.add('duplicates', [(c, {}), (other, {})], True)
        self.assertNotIn('duplicates', self.runtime.entries)
        self.assertNotIn(c.mac, self.runtime.clients)

    async def test_removed_route_retains_unreachable_from_control_history(self):
        self.learn()
        await self.command()
        self.runtime.state.learned(LearningResult(self.route, False, 7, 1.75, 'no reply'))
        self.assertEqual(self.b.bluetooth_status, 'unreachable')
        self.assertFalse(self.b.available)
        self.assertEqual(self.b.last_mesh_gateway, 1)

    async def test_gateway_decodes_once_for_multiple_target_stores(self):
        from awox112 import status_1911 as status
        await self.prepare()
        target = self.a.status_for_target(self.b.mac, 2)
        for mid in range(3, 10):
            self.a.status_for_target(f'02:00:00:00:00:{mid:02X}', mid)
        with patch.object(status, 'parse_status_1911', wraps=status.parse_status_1911) as decode:
            self.radios[1].emit(2)
        self.assertEqual(decode.call_count, 1)
        self.assertEqual(target.latest.mesh_id, 2)
        self.assertIsNone(self.a.status_1911.latest)
        for (_, mid), store in self.a._mesh_status_stores.items():
            self.assertEqual(bool(store.frames), mid == 2)

    async def test_decoded_envelope_still_requires_current_target_session(self):
        from awox112 import status_1911 as status
        raw = status.parse_status_1911(fx.KEY, fx.payload({}, mid=2, mac=self.b.mac, outer_type=0x82))
        store = Status1911Store(self.b.mac, 2, outer_type=0x82)
        store.begin(2)
        store.accept_decoded(raw, 1)
        self.assertIsNone(store.latest)
        store.end()
        store.accept_decoded(raw, 2)
        self.assertIsNone(store.latest)


if __name__ == '__main__':
    unittest.main(verbosity=2)
