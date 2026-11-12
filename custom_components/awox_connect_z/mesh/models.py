"""Identity-bound, directed routes; connection sessions are never persisted."""
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Route:
    gateway: str
    target: str
    gateway_id: int
    target_id: int


@dataclass(frozen=True, slots=True)
class MeshProof:
    gateway: str
    generation: int


@dataclass(frozen=True, slots=True)
class LearningResult:
    route: Route
    success: bool
    polls: int
    elapsed: float
    error: str | None = None
    attempts: int = 1
    successful_attempts: int | None = None

    @property
    def successes(self):
        return int(self.success) if self.successful_attempts is None else self.successful_attempts
