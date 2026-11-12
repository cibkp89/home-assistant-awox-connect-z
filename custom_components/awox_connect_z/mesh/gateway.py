"""Shared preparation and target-specific controls; exclusive learning probes."""
import asyncio
import time
from collections import defaultdict
from contextlib import AsyncExitStack, asynccontextmanager
from ..client import RuntimeCommandStatus


class _GatewayAccess:
    """Concurrent controls, with fair exclusive admission for learning."""

    def __init__(self):
        self.condition = asyncio.Condition()
        self.active = 0
        self.exclusive = False
        self.waiting_exclusive = 0

    @asynccontextmanager
    async def hold(self, shared):
        async with self.condition:
            if not shared:
                self.waiting_exclusive += 1
            try:
                await self.condition.wait_for(lambda: not self.exclusive and (
                    self.waiting_exclusive == 0 if shared else self.active == 0))
            finally:
                if not shared:
                    self.waiting_exclusive -= 1
                    self.condition.notify_all()
            self.active += 1
            self.exclusive = not shared
        try:
            yield
        finally:
            async with self.condition:
                self.active -= 1
                if not shared:
                    self.exclusive = False
                self.condition.notify_all()


class GatewayPool:
    def __init__(self, clients, state):
        self.clients = clients
        self.state = state
        self.locks = defaultdict(_GatewayAccess)
        self.users = {}
        self.control_users = set()
        self.target_locks = defaultdict(asyncio.Lock)
        self.preparations = {}

    def use_order(self, gateway):
        """A real control reservation is visible before its first await."""
        return next((index for index, (task, pair) in enumerate(self.users.items())
                     if task in self.control_users and pair[0] == gateway), float("inf"))

    async def prepare_shared(self, client):
        """Join one preparation; cancelling a waiter must not cancel its peers."""
        record = self.preparations.get(client.mac)
        if record is None:
            record = (asyncio.create_task(self.prepare(client), name="AwoX shared gateway preparation"), set())
            self.preparations[client.mac] = record
        task, waiters = record
        owner = asyncio.current_task()
        waiters.add(owner)
        try:
            return await asyncio.shield(task)
        finally:
            waiters.discard(owner)
            if not waiters:
                if self.preparations.get(client.mac) is record:
                    self.preparations.pop(client.mac)
                if not task.done():
                    task.cancel()
                # Retrieve failures and finish bounded BLE cleanup even when the
                # last waiter was cancelled. Remaining waiters keep the task alive.
                await asyncio.gather(task, return_exceptions=True)

    async def prepare(self, client):
        """Measure fresh connection preparation; never add attempts or writes."""
        if self.clients.get(client.mac) is not client or client.closed:
            raise ConnectionError("Mesh gateway was unloaded before preparation")
        fresh = not client.connected
        started = time.monotonic()
        outcome = "failed"
        try:
            result = await client.async_send_selected_once(lambda: None)
            outcome = "successful" if result.status is RuntimeCommandStatus.NOT_SELECTED else "failed"
            return result
        except asyncio.CancelledError:
            outcome = "cancelled"
            raise
        finally:
            direct_changed = (self.state.note_direct(client.mac, client.configured_mesh_id, "confirmed")
                              if outcome == "successful" else False)
            if fresh:
                previous = self.state.metrics.get(client.mac, {})
                metric = (dict(previous) if previous.get("mesh_id") == client.configured_mesh_id else
                          dict(mesh_id=client.configured_mesh_id, attempts=0, successful=0,
                               failed=0, cancelled=0, total_ms=0.0))
                elapsed = (time.monotonic() - started) * 1000
                metric["attempts"] += 1
                metric[outcome] += 1
                metric["total_ms"] += elapsed
                metric["last_ms"] = elapsed
                metric["min_ms"] = min(metric.get("min_ms", elapsed), elapsed)
                metric["max_ms"] = max(metric.get("max_ms", elapsed), elapsed)
                metric["rssi"] = client.signal_strength
                self.state.metrics[client.mac] = metric
                self.state.changed()
            elif direct_changed:
                self.state.changed()

    @asynccontextmanager
    async def reserve(self, gateway, target):
        """Exclusive access for a learning probe or maintenance operation."""
        async with self._reserve(gateway, target, shared=False) as client:
            yield client

    @asynccontextmanager
    async def control(self, gateway, target):
        """Keep one target ordered while other targets can send and confirm."""
        async with self._reserve(gateway, target, shared=True) as client:
            yield client


    @asynccontextmanager
    async def _reserve(self, gateway, target, *, shared):
        self._require_endpoints(gateway, target)
        task = asyncio.current_task()
        # Track queued users too: unloading either endpoint cancels their work.
        self.users[task] = (gateway, target)
        if shared:
            self.control_users.add(task)
        try:
            async with AsyncExitStack() as stack:
                # Waiting behind another group for this target must not occupy
                # a gateway reader slot and delay an unrelated learning probe.
                if shared:
                    await stack.enter_async_context(self.target_locks[target])
                await stack.enter_async_context(self.locks[gateway].hold(shared))
                client = self._require_endpoints(gateway, target)
                async with client.status_transaction():
                    yield client
        finally:
            self.users.pop(task, None)
            self.control_users.discard(task)

    def _require_endpoints(self, gateway, target):
        client = self.clients.get(gateway)
        destination = self.clients.get(target)
        if client is None or destination is None or client.closed or destination.closed:
            raise ConnectionError("Mesh gateway or target was unloaded")
        return client

    async def remove(self, addresses):
        tasks = [task for task, pair in tuple(self.users.items())
                 if set(pair) & addresses and task is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for address in addresses:
            self.locks.pop(address, None)
            self.target_locks.pop(address, None)
