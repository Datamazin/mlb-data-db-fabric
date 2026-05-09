"""
Pydantic models for /v1/venues response.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class _Coordinates(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)
    latitude: float | None = None
    longitude: float | None = None


class _Location(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)
    city: str | None = None
    state: str | None = None
    state_abbrev: str | None = Field(None, alias="stateAbbrev")
    country: str | None = None
    default_coordinates: _Coordinates | None = Field(None, alias="defaultCoordinates")


class _FieldInfo(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)
    capacity: int | None = None
    turf_type: str | None = Field(None, alias="turfType")
    roof_type: str | None = Field(None, alias="roofType")


class Venue(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)
    id: int
    name: str | None = None
    location: _Location | None = None
    field_info: _FieldInfo | None = Field(None, alias="fieldInfo")


class VenuesResponse(BaseModel):
    """Root response from /v1/venues."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)
    venues: list[Venue] = []
