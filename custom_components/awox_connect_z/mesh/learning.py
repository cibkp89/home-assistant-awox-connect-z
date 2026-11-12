"""Directed 1911 learning; no lamp commands or advertisement waits."""
import asyncio
import time
import logging
from dataclasses import asdict

from ..client import RuntimeCommandStatus
from ..const import CONFIRMATION_MAX_POLLS, CONFIRMATION_POLL_WINDOW_MS
from .models import LearningResult, Route

_LOGGER = logging.getLogger(__name__)
LEARNING_ROUNDS = 5


class MeshLearning:
    def __init__(self, runtime):
        self.runtime = runtime
        self.task = None
        self.results = []
        self.total = 0
        self.completed = 0
        self.round = 0
        self.current_route = None
        self.activity = "idle"
        self.cancelled = False
        self.error = None
        self.save_error = False
        self.listeners = set()
        self.report = {}

    def start(self, targets=None):
        if self.task is not None and not self.task.done():
            return False
        selected = set(self.runtime.enabled if targets is None else targets)
        if not selected or not selected <= self.runtime.enabled:
            raise ValueError("Select configured, mesh-enabled targets")
        self.results = []
        self.total = 0
        self.completed = 0
        self.round = 0
        self.activity = "preparing"
        self.cancelled = False
        self.error = None
        self.save_error = False
        self.report = {"targets": sorted(selected)}
        self.task = self.runtime.hass.async_create_background_task(self.run(selected), "AwoX mesh learning")
        return True

    def cancel(self):
        if self.task is not None and not self.task.done():
            self.cancelled = True
            self.activity = "cancelled"
            self.report["cancelled"] = True
            self._notify()
            self.task.cancel()

    def _notify(self):
        for listener in tuple(self.listeners):
            try:
                listener()
            except Exception:
                _LOGGER.exception("Mesh learning progress listener failed")

    def _set_activity(self, activity):
        self.activity = activity
        self._notify()

    async def stop(self):
        if self.task is not None and not self.task.done():
            self.cancel()
            await asyncio.gather(self.task, return_exceptions=True)

    async def run(self, targets=None):
        report = self.report
        clients = self.runtime.clients
        targets = sorted(self.runtime.enabled & set(self.runtime.enabled if targets is None else targets))
        gateways = [mac for mac in sorted(self.runtime.enabled)
                    if self.runtime.routing.usable(clients[mac])]
        routes = [Route(a, b, clients[a].configured_mesh_id, clients[b].configured_mesh_id)
                  for a in gateways for b in targets
                  if a != b and self.runtime.routing.permitted(a, b)]
        self.results = []
        self.completed = 0
        self.total = len(routes) * LEARNING_ROUNDS
        aggregated = {}

        def record(result):
            route = result.route
            previous = aggregated.get(route)
            successes = result.successes + (previous.successes if previous else 0)
            value = LearningResult(route, successes > 0,
                result.polls + (previous.polls if previous else 0),
                result.elapsed + (previous.elapsed if previous else 0),
                None if successes else result.error,
                (previous.attempts if previous else 0) + 1, successes)
            aggregated[route] = value
            self.results = list(aggregated.values())
            self.completed += 1
            if self.runtime.routing.permitted(route.gateway, route.target):
                self.runtime.state.learned(value)
            self._notify()

        try:
            for number in range(1, LEARNING_ROUNDS + 1):
                self.round = number
                for address in gateways:
                    selected = [r for r in routes if r.gateway == address
                                and self.runtime.routing.permitted(r.gateway, r.target)]
                    self.total -= sum(r.gateway == address for r in routes) - len(selected)
                    if not selected:
                        continue
                    self.current_route = selected[0]
                    self._set_activity("waiting_gateway")
                    async with self.runtime.pool.reserve(address, address) as gateway:
                        # Exclusive admission protects existing controls from the
                        # deliberate fresh connection used for each measurement.
                        try:
                            async with gateway._command_lock:
                                await gateway._async_disconnect(cancel_idle=True)
                            self._set_activity("connecting_gateway")
                            started = time.monotonic()
                            try:
                                result = await self.runtime.pool.prepare(gateway)
                                error = (None if result.status is RuntimeCommandStatus.NOT_SELECTED else
                                         str(result.error or "Gateway preparation failed"))
                            except Exception as exc:
                                error = str(exc)
                            if error is not None:
                                for route in selected:
                                    record(LearningResult(route, False, 0, time.monotonic()-started, error))
                                continue
                            for route in selected:
                                self.current_route = route
                                self._notify()
                                record(await self.probe(route, gateway=gateway))
                        finally:
                            async with gateway._command_lock:
                                await gateway._async_disconnect(cancel_idle=True, mark_liveness=True)
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        except Exception:
            self.error = "Unexpected learning error"
            _LOGGER.exception("AwoX mesh learning failed")
        finally:
            self.current_route = None
            self._set_activity("saving")
            try:
                await self.runtime.storage.flush()
            except asyncio.CancelledError:
                self.cancelled = True
                self.save_error = True
                raise
            except Exception:
                self.save_error = True
                _LOGGER.exception("Could not save AwoX mesh learning results")
            finally:
                report.update(self.diagnostics())
                report["running"] = False
                self.activity = "cancelled" if self.cancelled else "failed" if self.error else "done"
                self._notify()

    async def probe(self, route, *, gateway=None):
        if gateway is not None:
            return await self._probe_ready(route, gateway)
        started, polls = time.monotonic(), 0
        try:
            if not self.runtime.routing.permitted(route.gateway, route.target):
                raise ConnectionError("Mesh route is blocked")
            self._set_activity("waiting_gateway")
            async with self.runtime.pool.reserve(route.gateway, route.target) as gateway:
                if not self.runtime.routing.permitted(route.gateway, route.target):
                    raise ConnectionError("Mesh route was blocked while queued")
                self._set_activity("gateway_connected" if gateway.connected else "connecting_gateway")
                result = await self.runtime.pool.prepare(gateway)
                if result.status is not RuntimeCommandStatus.NOT_SELECTED:
                    raise result.error or ConnectionError("Gateway preparation failed")
                return await self._probe_ready(route, gateway)
        except Exception as error:
            return LearningResult(route, False, polls, time.monotonic() - started, str(error))

    async def _probe_ready(self, route, gateway):
        started, polls = time.monotonic(), 0
        try:
            store = gateway.status_for_target(route.target, route.target_id)
            generation = store.session_generation
            valid = lambda: (gateway.connected and not gateway.closed and
                             self.runtime.clients.get(route.gateway) is gateway and
                             route.target in self.runtime.clients and
                             gateway.configured_mesh_id == route.gateway_id and
                             self.runtime.clients[route.target].configured_mesh_id == route.target_id and
                             gateway.status_1911.session_generation == generation and
                             self.runtime.routing.permitted(route.gateway, route.target))
            # Cutoff follows preparation. No old buffered reply can learn a
            # route; the protocol has no nonce, so causal attribution remains
            # limited for late replies from this same target/session.
            after = time.monotonic()
            self._set_activity("testing_route")
            for _ in range(CONFIRMATION_MAX_POLLS):
                await gateway.async_poll_target_status(generation, route.target_id, valid)
                polls += 1
                state = await store.wait({}, generation, after,
                                         time.monotonic() + CONFIRMATION_POLL_WINDOW_MS / 1000,
                                         valid)
                if state is not None:
                    return LearningResult(route, True, polls, time.monotonic() - started)
            raise ConnectionError("No valid target reply")
        except Exception as error:
            return LearningResult(route, False, polls, time.monotonic() - started, str(error))

    def diagnostics(self):
        return {"running": bool(self.task and not self.task.done()), "total": self.total,
                "completed": self.completed, "round": self.round, "rounds": LEARNING_ROUNDS,
                "results": [asdict(r) for r in self.results],
                "cancelled": self.cancelled, "error": self.error, "save_error": self.save_error,
                "current_route": asdict(self.current_route) if self.current_route else None}
