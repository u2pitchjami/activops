from __future__ import annotations

from typing import Any, Protocol, TypedDict


# ---- Client minimal pour mypy (ce qu'on utilise vraiment) -------------------
class GarminClientProtocol(Protocol):
    def login(self) -> Any: ...
    def get_user_summary(self, date: str) -> dict[str, Any]: ...
    def get_daily_weigh_ins(self, date: str) -> list[dict[str, Any]]: ...
    def get_heart_rates(self, date: str) -> dict[str, Any]: ...
    def get_activities(self, start: int = 0, limit: int = 10) -> list[dict[str, Any]]: ...


# ---- Structures utilitaires -------------------------------------------------
class SummaryData(TypedDict, total=False):
    date: str
    calories: int
    steps: int
    stress: int
    intense_minutes: int
    sleep: float
    weight: float | None
    average_heart_rate: int | None
    last_sync: str | None  # "YYYY-mm-dd HH:MM:SS" local
    last_updated: str  # "YYYY-mm-dd HH:MM:SS"


class ActivityRow(TypedDict, total=False):
    activity_id: int | None
    activity_name: str
    activity_type: str
    startTimeLocal: str | None  # "YYYY-mm-dd HH:MM:SS"
    duration: float | None
    distance: float | None
    calories: int | None
    avg_heart_rate: int | None
    max_heart_rate: int | None
    steps: int | None
    elevation_gain: float | None
    elevation_loss: float | None
    average_speed: float | None
    max_speed: float | None
    moving_time: float | None
    elapsed_time: float | None
    averageRunningCadence: float | None
    maxRunningCadence: float | None
    hrTimeInZone_1: float | None
    hrTimeInZone_2: float | None
    hrTimeInZone_3: float | None
    hrTimeInZone_4: float | None
    hrTimeInZone_5: float | None
    minTemperature: float | None
    maxTemperature: float | None
    ownerId: int | None
    ownerDisplayName: str
    ownerFullName: str
    deviceId: int | None
    manufacturer: str
    startLatitude: float | None
    startLongitude: float | None
    locationName: str
    json_data: str
