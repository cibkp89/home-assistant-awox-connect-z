"""Persist identity-bound routes, rules and measurements per mesh network."""
import math
from datetime import datetime
from .models import LearningResult, Route


def _mac(value):
    try:
        return isinstance(value, str) and len(bytes.fromhex(value.replace(":", ""))) == 6
    except ValueError:
        return False


def _mid(value):
    return type(value) is int and 1 <= value <= 0xFFFE


def _route(value):
    try:
        route = Route(**value)
        if (route.gateway != route.target and all(_mac(v) for v in (route.gateway, route.target))
                and all(_mid(v) for v in (route.gateway_id, route.target_id))):
            return route
    except (TypeError, ValueError):
        pass


def _duration(value):
    try:
        return type(value) in (int, float) and math.isfinite(value) and value >= 0
    except OverflowError:
        return False


class MeshStorage:
    def __init__(self, hass, network_key, state):
        from homeassistant.helpers.storage import Store
        self.store = Store(hass, 1, f"awox_connect_z.mesh.{network_key}")
        self.state = state

    async def load(self):
        data = await self.store.async_load()
        if not isinstance(data, dict):
            return
        routes = data.get("routes", [])
        for item in routes if isinstance(routes, list) else []:
            route = _route(item)
            if route is not None:
                self.state.routes[(route.gateway, route.target)] = route
        history = data.get("last_gateways", {})
        if isinstance(history, dict):
            self.state.last_gateways.update({
                mac: mid for mac, mid in history.items()
                if _mac(mac) and _mid(mid)
            })
        results = data.get("learning_results", [])
        for item in results if isinstance(results, list) else []:
            if not isinstance(item, dict):
                continue
            route = _route(item.get("route"))
            attempts = item.get("attempts", 1)
            successes = item.get("successful_attempts")
            if successes is None:
                successes = int(item.get("success") is True)
            if (route is not None and type(item.get("success")) is bool
                    and type(attempts) is int and 1 <= attempts <= 5
                    and type(successes) is int and 0 <= successes <= attempts
                    and item["success"] == (successes > 0)
                    and type(item.get("polls")) is int and 0 <= item["polls"] <= 65535
                    and _duration(item.get("elapsed"))
                    and (item.get("error") is None or isinstance(item["error"], str))):
                self.state.learning_results[(route.gateway, route.target)] = LearningResult(
                    route, item["success"], item["polls"], item["elapsed"], item.get("error"),
                    attempts, successes)
        preferences = data.get("preferences", {})
        for target, item in preferences.items() if isinstance(preferences, dict) else []:
            if not _mac(target) or not isinstance(item, dict) or not _mid(item.get("target_id")):
                continue
            preferred, blocked, ids = item.get("preferred"), item.get("blocked"), item.get("gateway_ids")
            if (not isinstance(blocked, list) or not all(_mac(mac) for mac in blocked)
                    or preferred is not None and not _mac(preferred)
                    or preferred in blocked or not isinstance(ids, dict)
                    or not all(_mac(mac) and _mid(mid) for mac, mid in ids.items())
                    or not set(blocked + ([preferred] if preferred else [])) <= ids.keys()):
                continue
            selected = set(blocked) | ({preferred} if preferred else set())
            if selected:
                self.state.preferences[target] = dict(target_id=item["target_id"], preferred=preferred,
                    blocked=sorted(set(blocked)), gateway_ids={mac: ids[mac] for mac in sorted(selected)})
        metrics = data.get("metrics", {})
        for mac, item in metrics.items() if isinstance(metrics, dict) else []:
            if not _mac(mac) or not isinstance(item, dict) or not _mid(item.get("mesh_id")):
                continue
            counts = ("attempts", "successful", "failed", "cancelled")
            durations = ("total_ms", "last_ms", "min_ms", "max_ms")
            if (not all(type(item.get(k)) is int and item[k] >= 0 for k in counts)
                    or item["attempts"] != sum(item[k] for k in counts[1:])
                    or not all(_duration(item.get(k)) for k in durations)
                    or item.get("rssi") is not None and type(item["rssi"]) is not int):
                continue
            self.state.metrics[mac] = {key: item[key] for key in ("mesh_id", *counts, *durations)}
            self.state.metrics[mac]["rssi"] = item.get("rssi")
        history = data.get("route_history", [])
        for item in history if isinstance(history, list) else []:
            route = _route(item)
            if route is not None:
                self.state.route_history[(route.gateway, route.target)] = route
        self.state.route_history.update(self.state.routes)
        direct = data.get("direct", {})
        for mac, item in direct.items() if isinstance(direct, dict) else []:
            if (_mac(mac) and isinstance(item, dict) and _mid(item.get("mesh_id"))
                    and type(item.get("confirmed")) is bool
                    and item.get("last_status") in ("confirmed", "error", "not_checked")):
                self.state.direct[mac] = dict(mesh_id=item["mesh_id"],confirmed=item["confirmed"],
                                               last_status=item["last_status"])
        ble = data.get("ble_metrics", {})
        for mac, item in ble.items() if isinstance(ble, dict) else []:
            if not _mac(mac) or not isinstance(item, dict) or not _mid(item.get("mesh_id")):
                continue
            counts = ("attempts", "successful", "failed", "cancelled")
            durations, recent = item.get("durations_ms"), item.get("recent_attempts")
            if (not all(type(item.get(key)) is int and item[key] >= 0 for key in counts)
                    or item["attempts"] != sum(item[key] for key in counts[1:])
                    or not isinstance(durations,list) or len(durations)>50 or not all(_duration(v) for v in durations)
                    or not isinstance(recent,list) or len(recent)>50
                    or not all(v in ("successful","failed","cancelled") for v in recent)
                    or item.get("last_rssi") is not None and type(item["last_rssi"]) is not int
                    or item.get("last_outcome") not in ("successful","failed","cancelled")):
                continue
            self.state.ble_metrics[mac] = {key:item[key] for key in ("mesh_id",*counts)}
            self.state.ble_metrics[mac].update(durations_ms=list(durations),recent_attempts=list(recent),
                last_rssi=item.get("last_rssi"),last_outcome=item["last_outcome"])
        started = data.get("ble_metrics_started_at")
        if isinstance(started,str):
            try:
                if datetime.fromisoformat(started).tzinfo is not None:
                    self.state.ble_metrics_started_at = started
            except ValueError:
                pass

    def schedule(self):
        self.store.async_delay_save(self.state.export, 1)

    async def flush(self):
        await self.store.async_save(self.state.export())
