"""Wire one credential-isolated mesh runtime across compatible config entries."""
import asyncio
import hashlib
import json
import logging

from .dispatch import MeshDispatch
from .gateway import GatewayPool
from .learning import MeshLearning
from .routing import MeshRouting
from .state import MeshState
from .storage import MeshStorage

DATA_MESH = "awox_connect_z_mesh"
DATA_MESH_LOCK = "awox_connect_z_mesh_registry_lock"
_LOGGER = logging.getLogger(__name__)


def network_key(name, password):
    return hashlib.sha256(json.dumps([name, password]).encode()).hexdigest()


class MeshRuntime:
    def __init__(self, hass, key):
        self.hass = hass
        self.key = key
        self.clients = {}
        self.devices = {}
        self.enabled = set()
        self.entries = {}
        self.unsubscribers = {}
        self.state = MeshState()
        self.pool = GatewayPool(self.clients, self.state)
        self.routing = MeshRouting(self.state, self.clients, self.enabled, self.pool)
        self.storage = MeshStorage(hass, key, self.state)
        self.learning = MeshLearning(self)
        self._publishing = False
        self._lock = asyncio.Lock()
        self._loaded = False
        self.state.listeners.update((self.refresh, self.storage.schedule))

    async def add(self, entry_id, clients, enabled):
        async with self._lock:
            if not self._loaded:
                await self.storage.load()
                self._loaded = True
            addresses = {client.mac for client, _ in clients}
            if len(addresses) != len(clients) or addresses & self.clients.keys():
                raise ValueError("A lamp cannot belong to two loaded mesh entries")
            mesh_ids = [client.configured_mesh_id for client in self.clients.values()]
            mesh_ids.extend(client.configured_mesh_id for client, _ in clients)
            if len(mesh_ids) != len(set(mesh_ids)):
                raise ValueError("Duplicate lamp mesh destinations within one network")
            self.entries[entry_id] = addresses
            # A reused MAC with a changed destination must learn fresh routes.
            for client, _ in clients:
                for key, route in tuple(self.state.routes.items()):
                    if ((route.target == client.mac and route.target_id != client.configured_mesh_id)
                            or (route.gateway == client.mac and route.gateway_id != client.configured_mesh_id)):
                        self.state.routes.pop(key)
                        self.state.last_gateways.pop(client.mac, None)
            for client, device in clients:
                self.clients[client.mac] = client
                self.devices[client.mac] = dict(device)
                client._ble_connection_observer = lambda outcome, duration, rssi, c=client: self._note_ble(c, outcome, duration, rssi)
                if enabled:
                    self.enabled.add(client.mac)
                client.mesh_dispatch = MeshDispatch(self, client)
                client.mesh_status_provider = lambda mac=client.mac: self.routing.status(mac)
                client.mesh_history_provider = lambda mac=client.mac: self.state.last_gateways.get(mac)
                self.unsubscribers[client.mac] = client.async_add_diagnostic_listener(self.refresh)
            self.refresh()

    def refresh(self):
        if self._publishing:
            return
        self._publishing = True
        try:
            for client in tuple(self.clients.values()):
                client.async_refresh_mesh_status()
        finally:
            self._publishing = False

    def _note_ble(self, client, outcome, duration, rssi):
        self.state.note_ble(client.mac, client.configured_mesh_id, outcome, duration, rssi)
        # Do not publish a connected light before authentication has finished.
        self.storage.schedule()

    async def remove(self, entry_id):
        async with self._lock:
            addresses = self.entries.pop(entry_id, set())
            # Close admission before the first await. Cleanup may yield while
            # older requests release a gateway; no new lease may enter then.
            removed = {address: self.clients.pop(address, None) for address in addresses}
            self.enabled.difference_update(addresses)
            await self.learning.stop()
            await self.pool.remove(addresses)
            for address, client in removed.items():
                self.devices.pop(address, None)
                unsubscribe = self.unsubscribers.pop(address, None)
                if unsubscribe is not None:
                    unsubscribe()
                if client is None:
                    continue
                client._ble_connection_observer = None
                client.mesh_dispatch = None
                client.mesh_status_provider = None
                client.mesh_history_provider = None
                self.state.proofs.pop(address, None)
            for target, proof in tuple(self.state.proofs.items()):
                if proof.gateway in addresses:
                    self.state.proofs.pop(target)
            for client in self.clients.values():
                for address in addresses:
                    client.forget_status_target(address)
            self.refresh()
            try:
                await self.storage.flush()
            except Exception:
                # Storage failure must not prevent physical client cleanup.
                _LOGGER.exception("Could not save AwoX mesh route history")

    def diagnostics(self):
        return {**self.state.export(), "learning": self.learning.diagnostics(),
                "enabled_devices": sorted(self.enabled),
                "active_leases": len(self.pool.users),
                "active_preparations": len(self.pool.preparations)}


async def async_attach(hass, entry, clients):
    # Serialize registry lifetime, not control. Otherwise a concurrent last
    # detach can remove a runtime while a new entry is waiting to join it.
    async with hass.data.setdefault(DATA_MESH_LOCK, asyncio.Lock()):
        return await _attach(hass, entry, clients)


async def _attach(hass, entry, clients):
    key = network_key(entry.data["mesh_name"], entry.data["mesh_password"])
    runtimes = hass.data.setdefault(DATA_MESH, {})
    if key not in runtimes:
        runtimes[key] = MeshRuntime(hass, key)
    runtime = runtimes[key]
    registered_before = entry.entry_id in runtime.entries
    try:
        await runtime.add(entry.entry_id, clients, entry.options.get("mesh_enabled", True))
    except BaseException:
        if not registered_before and entry.entry_id in runtime.entries:
            await runtime.remove(entry.entry_id)
        if not runtime.entries:
            runtimes.pop(key, None)
        raise
    return runtime


async def async_detach(hass, entry_id):
    async with hass.data.setdefault(DATA_MESH_LOCK, asyncio.Lock()):
        await _detach(hass, entry_id)


async def _detach(hass, entry_id):
    runtimes = hass.data.get(DATA_MESH, {})
    for key, runtime in tuple(runtimes.items()):
        if entry_id in runtime.entries:
            await runtime.remove(entry_id)
            if not runtime.entries:
                runtimes.pop(key, None)
    if not runtimes:
        hass.data.pop(DATA_MESH, None)
