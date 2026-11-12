"""Shared network facts, separate from transport and availability policy."""
from dataclasses import asdict
import logging
from datetime import datetime, timezone
from .models import LearningResult, MeshProof, Route

_LOGGER = logging.getLogger(__name__)


class MeshState:
    def __init__(self):
        self.routes: dict[tuple[str, str], Route] = {}
        self.proofs: dict[str, MeshProof] = {}
        self.last_gateways: dict[str, int] = {}
        self.failures: dict[tuple[str, str], int] = {}
        self.learning_results: dict[tuple[str, str], LearningResult] = {}
        self.preferences: dict[str, dict] = {}
        self.metrics: dict[str, dict] = {}
        self.ble_metrics: dict[str, dict] = {}
        self.ble_metrics_started_at = None
        self.direct: dict[str, dict] = {}
        self.route_history: dict[tuple[str, str], Route] = {}
        self.listeners = set()

    def changed(self):
        for listener in tuple(self.listeners):
            try:
                listener()
            except Exception:
                # A failed save/publication must not undo confirmed control or
                # prevent the other subscribers from seeing the changed facts.
                _LOGGER.exception("AwoX mesh state listener failed")

    def learned(self, result: LearningResult):
        route = result.route
        key = (route.gateway, route.target)
        self.learning_results[key] = result
        if result.success:
            self.routes[key] = route
            self.route_history[key] = route
            self.failures.pop(key, None)
        else:
            # A failed revalidation revokes this direction, not its reverse.
            self.routes.pop(key, None)
            self.failed(route)
        self.changed()

    def failed(self, route: Route):
        key = (route.gateway, route.target)
        self.failures[key] = self.failures.get(key, 0) + 1
        proof = self.proofs.get(route.target)
        if proof is not None and proof.gateway == route.gateway:
            self.proofs.pop(route.target, None)

    def confirmed(self, route: Route, generation: int):
        self.proofs[route.target] = MeshProof(route.gateway, generation)
        self.last_gateways[route.target] = route.gateway_id
        self.failures.pop((route.gateway, route.target), None)
        self.changed()

    def export(self):
        return {
            "routes": [asdict(route) for route in self.routes.values()],
            "last_gateways": dict(self.last_gateways),
            "learning_results": [asdict(result) for result in self.learning_results.values()],
            "preferences": dict(self.preferences),
            "metrics": dict(self.metrics),
            "ble_metrics": dict(self.ble_metrics),
            "ble_metrics_started_at": self.ble_metrics_started_at,
            "direct": dict(self.direct),
            "route_history": [asdict(route) for route in self.route_history.values()],
        }

    def note_direct(self, mac, mesh_id, status):
        old = self.direct.get(mac, {})
        confirmed = old.get("confirmed", False) if old.get("mesh_id") == mesh_id else False
        value = dict(mesh_id=mesh_id, confirmed=confirmed or status == "confirmed", last_status=status)
        self.direct[mac] = value
        return old != value

    def note_ble(self, mac, mesh_id, outcome, duration_ms, rssi):
        previous = self.ble_metrics.get(mac, {})
        value = (dict(previous) if previous.get("mesh_id") == mesh_id else
                 dict(mesh_id=mesh_id, attempts=0, successful=0, failed=0, cancelled=0,
                      durations_ms=[], recent_attempts=[]))
        value["attempts"] += 1
        value[outcome] += 1
        now = datetime.now(timezone.utc).isoformat()
        value["durations_ms"] = ([*value["durations_ms"], duration_ms][-50:]
                                 if outcome == "successful" else list(value["durations_ms"]))
        value["recent_attempts"] = [*value["recent_attempts"], outcome][-50:]
        value.update(last_rssi=rssi, last_outcome=outcome)
        self.ble_metrics[mac] = value
        if self.ble_metrics_started_at is None:
            self.ble_metrics_started_at = now
        self.note_direct(mac, mesh_id, "confirmed" if outcome == "successful" else
                         "not_checked" if outcome == "cancelled" else "error")

    def set_preferences(self, target, target_id, preferred, blocked, gateway_ids):
        if preferred is not None and preferred in blocked:
            raise ValueError("Preferred gateway cannot also be blocked")
        blocked = sorted(set(blocked))
        if preferred is None and not blocked:
            if target not in self.preferences:
                return
            self.preferences.pop(target)
        else:
            selected = set(blocked) | ({preferred} if preferred else set())
            value = dict(target_id=target_id, preferred=preferred, blocked=blocked,
                         gateway_ids={mac: gateway_ids[mac] for mac in sorted(selected)})
            if self.preferences.get(target) == value:
                return
            self.preferences[target] = value
        self.changed()
