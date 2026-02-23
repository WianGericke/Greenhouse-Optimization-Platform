from __future__ import annotations

from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import Any, Dict, List, Optional

Severity = float # v1 convention 0.0 ... 1.0

@dataclass(frozen=True)
class Event:
    """
    v1 Events
    Used to log detections made by modules in a clear and consice manner

    Deign Goals:
        - minimal required fields
        - Stable internal structure across platform
        - Extendable via optional fields to include more information
    """

    # Required Fields
    event_type: str
    severity: Severity
    start_time: datetime
    end_time: datetime
    source: str 

    # Optional Fields
    # IDs
    greenhouse_id: Optional[str] = None
    zone_id: Optional[str] = None

    # Other
    metrics: Optional[Dict[str, Any]] = None
    explanation: Optional[str] = None
    recommended_actions: Optional[List[str]] = None

    # Future-proofing hook: any extra metadata goes here
    extra: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["start_time"] = self.start_time.isoformat()
        d["end_time"] = self.end_time.isoformat()
        return d
    
@dataclass(frozen=True)
class PlanStep:
    """
    A plan step is an advisory proposition in v1, no hardware control yet...
    """
    start_time: datetime
    end_time: datetime
    action_type: str
    details: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["start_time"] = self.start_time.isoformat()
        d["end_time"] = self.end_time.isoformat()
        return d

@dataclass(frozen=True)
class Plan:
    """
    A plan is a time indexed suggestion derived from events
    """
    plan_type: str
    source: str

    greenhouse_id: Optional[str] = None
    zone_id: Optional[str] = None

    steps: List[PlanStep] = field(default_factory=list)
    notes: Optional[List[str]] = None
    extra: Optional[Dict[str, Any]] = None

    def __post_init__(self) -> None:
        if self.steps is None:
            object.__setattr__(self, "steps", [])
        if self.notes is None:
            object.__setattr__(self, "notes", [])

    def to_dict(self) -> Dict[str, Any]:
        return {
            "plan_type": self.plan_type,
            "source": self.source,
            "greenhouse_id": self.greenhouse_id,
            "zone_id": self.zone_id,
            "steps": [s.to_dict() for s in self.steps],
            "notes": self.notes,
            "extra": self.extra,
        }