import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Dict

from storage.definitions import Process

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)

logging.getLogger("openeo.rest.connection").setLevel(logging.DEBUG)


@dataclass(frozen=True)
class OpenEOConfig:
    backend: str
    oidc_provider: str


class SpatialExtent(Dict):
    west: float
    south: float
    east: float
    north: float


@dataclass()
class Collection:
    name: str
    spatial_extent: SpatialExtent
    bands: Iterable[str]

    temporal_extent: (datetime.date, datetime.date)


@dataclass(frozen=True)
class OpenEOProcess:
    process: Process
    collection: Collection
