"""Pydantic models for MLB Stats API seasons endpoint."""

from pydantic import BaseModel, ConfigDict, Field


class SeasonMetadata(BaseModel):
    """Single season metadata from /v1/seasons."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    season_id: int = Field(alias="seasonId")
    regular_season_start_date: str = Field(alias="regularSeasonStartDate")
    regular_season_end_date: str = Field(alias="regularSeasonEndDate")
    preseason_start_date: str | None = Field(None, alias="preSeasonStartDate")
    postseason_start_date: str | None = Field(None, alias="postSeasonStartDate")
    world_series_end_date: str | None = Field(None, alias="worldSeriesEndDate")
    sport_id: int | None = Field(None, alias="sportId")
    games_per_team: int | None = Field(None, alias="gamesPerTeam")


class SeasonsResponse(BaseModel):
    """Response from /v1/seasons?season={year}&sportId=1."""

    model_config = ConfigDict(extra="ignore")

    seasons: list[SeasonMetadata]
