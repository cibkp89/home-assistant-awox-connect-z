"""Target-specific transport adapter for the existing command executor."""
import time
from contextlib import AsyncExitStack, asynccontextmanager

from ..command.confirmation import CommandConfirmation
from ..const import CONFIRMATION_MAX_POLLS, CONFIRMATION_POLL_WINDOW_MS
from ..client import RuntimeCommandStatus, CommandPreparationFailed


class MeshDispatch:
    def __init__(self, runtime, target):
        self.runtime, self.target = runtime, target
        self.route = None
        self.gateway = None
        self.store = None
        self.generation = None
        self.confirmation = CommandConfirmation(self)

    @property
    def default_transition(self):
        return self.target.default_transition

    @property
    def closed(self):
        return self.target.closed

    @property
    def connected(self):
        return bool(self.gateway and not self.gateway.closed and self.gateway.connected and not self.closed)

    @property
    def status_1911(self):
        return self.store if self.store is not None else self.target.status_1911

    @asynccontextmanager
    async def status_transaction(self):
        if self.route is not None:
            raise RuntimeError("Overlapping command groups for the same target")
        stack = AsyncExitStack()
        last_error = None
        try:
            # The configured first path plus at most one distinct alternative.
            attempted = set()
            for _ in range(2):
                # Re-evaluate still-unsent alternatives: another action may have
                # established a usable gateway while the first attempt failed.
                choices = [candidate for candidate in self.runtime.routing.candidates(self.target.mac)
                           if candidate.gateway not in attempted]
                if not choices:
                    break
                route = choices[0]
                attempted.add(route.gateway)
                try:
                    gateway = await stack.enter_async_context(
                        self.runtime.pool.control(route.gateway, route.target)
                    )
                    if (not self.runtime.routing.permitted(route.gateway, route.target) or
                            route.gateway != route.target and not self.runtime.routing.allowed(route)):
                        raise ConnectionError("Mesh route changed while queued")
                    # Share preparation without selecting a lamp command. Target
                    # reply waits may overlap; physical writes keep the client's
                    # lock. Only preparation failure may change the gateway.
                    result = await self.runtime.pool.prepare_shared(gateway)
                    if result.status is not RuntimeCommandStatus.NOT_SELECTED:
                        raise result.error or ConnectionError("Gateway preparation failed")
                    store = gateway.status_for_target(self.target.mac, route.target_id)
                    self.route, self.gateway, self.store = route, gateway, store
                    self.generation = store.session_generation
                    break
                except Exception as error:
                    last_error = error
                    await stack.aclose()
                    if route.gateway != route.target:
                        self.runtime.state.failed(route)
            if self.route is None:
                raise CommandPreparationFailed(str(last_error or "No usable route")) from last_error
            try:
                yield
            except BaseException:
                if self.route.gateway != self.route.target:
                    self.runtime.state.failed(self.route)
                    self.runtime.state.changed()
                raise
        finally:
            self.route = self.gateway = self.store = self.generation = None
            await stack.aclose()

    def _check(self, generation):
        if (self.route is None or self.closed or not self.connected or
                self.runtime.clients.get(self.route.gateway) is not self.gateway or
                self.runtime.clients.get(self.route.target) is not self.target or
                self.gateway.configured_mesh_id != self.route.gateway_id or
                self.target.configured_mesh_id != self.route.target_id or
                generation != self.generation or not self.store.active or
                self.store.session_generation != generation or
                self.gateway.status_1911.session_generation != generation):
            raise ConnectionError("Mesh command session ended; no automatic reroute")
        if (not self.runtime.routing.permitted(self.route.gateway, self.route.target) or
                self.route.gateway != self.route.target and not self.runtime.routing.allowed(self.route)):
            raise ConnectionError("Mesh route is no longer allowed")

    async def async_send_selected_once(self, selector, *, expected_session=None):
        started = time.monotonic()
        generation = self.generation if expected_session is None else expected_session
        self._check(generation)

        def guarded():
            # Called under the physical client's write lock, immediately before
            # encryption/write; queued configuration changes cannot slip through.
            self._check(generation)
            return selector()

        result = await self.gateway.async_send_selected_once(guarded, expected_session=generation)
        if self.route.gateway != self.route.target:
            if result.status is RuntimeCommandStatus.SUCCESS:
                self.target._record_command_success(result.label, started_monotonic=started,
                                                    attempts=result.attempt or 1)
            elif result.error is not None:
                self.target._record_error(result.error, category="mesh_transport", stage="write",
                                          command_label=result.label, write_attempt=result.attempt)
        return result

    async def async_poll_status(self, generation):
        self._check(generation)
        await self.gateway.async_poll_target_status(generation, self.route.target_id,
                                                   lambda: self._valid(generation))

    def _valid(self, generation):
        try:
            self._check(generation)
        except ConnectionError:
            return False
        return True

    async def async_repeat_plaintext(self, frame, generation, valid):
        self._check(generation)
        return await self.gateway.async_repeat_plaintext(
            frame, generation, lambda: valid() and self._valid(generation),
        )

    async def async_status_delay(self, seconds, valid):
        await self.gateway.async_status_delay(seconds, valid)

    def note_confirmed(self):
        self._check(self.generation)
        if self.route.gateway != self.route.target:
            self.runtime.state.confirmed(self.route, self.generation)

    async def async_initial_status(self):
        async with self.status_transaction():
            started = time.monotonic()
            for _ in range(CONFIRMATION_MAX_POLLS):
                await self.async_poll_status(self.generation)
                self.target._initial_status_polls += 1
                state = await self.store.wait(
                    {}, self.generation, started,
                    time.monotonic() + CONFIRMATION_POLL_WINDOW_MS / 1000,
                    lambda: self._valid(self.generation),
                )
                if state is not None:
                    return state
            raise ConnectionError("No valid target initial status within the poll budget")
