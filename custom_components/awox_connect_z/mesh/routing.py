"""Pure path selection; a usable gateway must have direct radio evidence."""
from .models import Route
from statistics import median


class MeshRouting:
    def __init__(self, state, clients, enabled, pool):
        self.state, self.clients, self.enabled = state, clients, enabled
        self.pool = pool

    def allowed(self, route):
        gateway, target = self.clients.get(route.gateway), self.clients.get(route.target)
        return bool(
            gateway and target and not gateway.closed and not target.closed
            and route.gateway in self.enabled and route.target in self.enabled
            and gateway.configured_mesh_id == route.gateway_id
            and target.configured_mesh_id == route.target_id
            and self.state.routes.get((route.gateway, route.target)) == route
            and self.permitted(route.gateway, route.target)
        )

    def preference(self, target):
        saved = self.state.preferences.get(target, {})
        client = self.clients.get(target)
        return saved if client and saved.get("target_id") == client.configured_mesh_id else {}

    def permitted(self, gateway, target):
        if target not in self.enabled:
            return True  # Disabling mesh restores unrestricted direct control.
        saved = self.preference(target)
        client = self.clients.get(gateway)
        # A new device at a reused mesh destination must never inherit a rule.
        return not (gateway in saved.get("blocked", []) and client
                    and saved.get("gateway_ids", {}).get(gateway) == client.configured_mesh_id)

    def usable(self, gateway):
        return not gateway.closed and gateway.direct_status in (
            "connected", "visible", "recently_connected"
        )

    def mesh_routes(self, target):
        return sorted(
            (route for route in self.state.routes.values()
             if route.target == target and self.allowed(route)
             and self.usable(self.clients[route.gateway])),
            key=lambda route: (not self.clients[route.gateway].connected,
                               self.state.failures.get((route.gateway, route.target), 0),
                               route.gateway),
        )

    def candidates(self, target):
        client = self.clients[target]
        direct = Route(target, target, client.configured_mesh_id, client.configured_mesh_id)
        # Reachability display requires fresh evidence. An explicit connection
        # attempt may still use a known identity-bound route without fresh radio
        # evidence, as in the old coordinator. Never invent an unlearned route.
        routes = [route for route in self.state.routes.values()
                  if route.target == target and self.allowed(route)]
        candidates = [direct, *routes]
        candidates = [route for route in candidates if self.permitted(route.gateway, target)]
        candidates.sort(key=self._candidate_key)
        saved = self.preference(target) if target in self.enabled else {}
        preferred = saved.get("preferred")
        gateway = self.clients.get(preferred)
        if gateway and saved.get("gateway_ids", {}).get(preferred) == gateway.configured_mesh_id:
            candidates.sort(key=lambda route: route.gateway != preferred)
        return candidates

    def _candidate_key(self, route):
        client = self.clients[route.gateway]
        record = self.state.ble_metrics.get(route.gateway, {})
        if record.get("mesh_id") != client.configured_mesh_id:
            record = {}
        recent = [outcome for outcome in record.get("recent_attempts", []) if outcome != "cancelled"]
        successes = recent.count("successful")
        measured = len(recent) >= 5
        rate = successes / len(recent) if measured else 0
        durations = record.get("durations_ms", [])
        quality = 0 if measured and rate >= .8 else 2 if measured else 1
        rssi = record.get("last_rssi")
        return (not (client.connected and client.status_1911.active),
                self.pool.use_order(route.gateway), not self.usable(client),
                self.state.failures.get((route.gateway, route.target), 0),
                quality, -rate,
                median(durations) if measured and durations else float("inf"),
                -rssi if measured and rssi is not None else float("inf"),
                route.gateway != route.target, route.gateway_id)

    def status(self, target):
        routes = self.mesh_routes(target)
        proof = self.state.proofs.get(target)
        if proof is not None:
            for route in routes:
                gateway = self.clients[route.gateway]
                if (proof.gateway == route.gateway and gateway.connected and
                        gateway.status_1911.active and
                        gateway.status_1911.session_generation == proof.generation):
                    return "mesh_connected"
        if routes:
            return "mesh_available"
        known = target in self.state.last_gateways or any(
            r.target == target for r in self.state.routes.values()
        )
        return "unreachable" if known else None
