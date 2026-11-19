from datetime import UTC, datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

SOURCE = "VIIRS_NOAA20_NRT"
TOPIC = "firms.detections.v1"


class Detection(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    schema_version: Literal[1]
    source: Literal["VIIRS_NOAA20_NRT"]
    satellite: str = Field(min_length=1, max_length=32)
    observed_at: datetime
    latitude: Decimal = Field(ge=-90, le=90)
    longitude: Decimal = Field(ge=-180, le=180)
    attributes: dict[str, str]

    @field_validator("satellite")
    @classmethod
    def satellite_not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("satellite must not be blank")
        return value.strip()

    @field_validator("observed_at")
    @classmethod
    def timestamp_is_utc(cls, value: datetime) -> datetime:
        if value.utcoffset() is None:
            raise ValueError("observed_at must include a timezone")
        return value.astimezone(UTC)
