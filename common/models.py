from typing import Any, Optional

from pydantic import BaseModel, Field


class DataPayload(BaseModel):
    """Key/value write. Sensitive clinical keys are placed only on the private transmitter."""

    key: str
    value: Any
    is_sensitive: bool = False


class SystemHealth(BaseModel):
    status: str
    node_id: str
    role: str
    zone_id: str = ""
    is_private: bool = False
    stored_keys_count: int = 0
    residents_count: int = 0


class DistributedResponse(BaseModel):
    status: str
    node_id: str
    message: str
    data: Optional[dict[str, Any]] = None
    trace_id: str
    routed_to: Optional[str] = None
    network_zone: Optional[str] = None


class ResidentRecord(BaseModel):
    resident_id: str = Field(pattern=r"^[A-Za-z0-9_-]{2,32}$")
    display_name: str = Field(min_length=1, max_length=80)
    zone_id: str = Field(pattern=r"^zone-[ab]$")
    care_level: str = Field(default="standard", pattern=r"^(low|standard|high)$")
    notes: str = ""


class SenseRequest(BaseModel):
    resident_id: str
    scenario: str = Field(
        default="resting",
        pattern=r"^(resting|warning|emergency|fall|walking)$",
    )
    duration_s: float = Field(default=20.0, ge=12.0, le=24.0)

    model_config = {
        "json_schema_extra": {
            "examples": [
                {"resident_id": "P001", "scenario": "resting"},
                {"resident_id": "P002", "scenario": "fall"},
            ]
        }
    }


class CsiLink(BaseModel):
    receiver_id: str
    link_id: str
    gain: float
    amplitudes: list[list[float]]


class TransmitRequest(BaseModel):
    resident_id: str
    zone_id: str
    scenario: str
    duration_s: float = 20.0
    session_id: Optional[str] = None


class TransmitResponse(BaseModel):
    session_id: str
    node_id: str
    scenario: str
    zone_id: str
    fs_hz: float
    n_samples: int
    n_subcarriers: int
    ground_truth: dict[str, Any]
    links: list[CsiLink]
    transmit_ms: float
    trace_id: str


class ProcessRequest(BaseModel):
    session_id: str
    link_id: str
    receiver_id: str
    zone_id: str
    fs_hz: float
    amplitudes: list[list[float]]


class LinkEstimate(BaseModel):
    node_id: str
    role: str
    zone_id: str
    link_id: str
    session_id: str
    heart_bpm: float
    breath_bpm: float
    heart_confidence: float
    breath_confidence: float
    movement: str
    activity: str
    fall_detected: bool
    motion_gated: bool
    vitals_reliable: bool
    motion_rms: float
    selected_subcarriers: list[int]
    n_samples: int
    n_subcarriers: int
    fs_hz: float
    processing_ms: float
    trace_id: str


class AlertRecord(BaseModel):
    alert_id: str
    session_id: str
    resident_id: str
    resident_name: str
    zone_id: str
    care_level: str
    urgency: str
    summary: str
    tags: list[str]
    heart_bpm: float
    breath_bpm: float
    activity: str
    fall_detected: bool
    trace_id: str
    created_at: str
