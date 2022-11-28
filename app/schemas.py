from dataclasses import dataclass
from typing import Any, List, Optional


@dataclass
class DataFrameSummary:
    name: str
    columns: List[str]
    rows_count: int


@dataclass
class DataFrameSummaryList:
    count: int
    loaded_data: List[DataFrameSummary]


@dataclass
class AircraftModelBase:
    manufacturer: str
    model: str


@dataclass
class AircraftModel(AircraftModelBase):
    seats: int


@dataclass
class AircraftModelList:
    count: int
    loaded_data: List[AircraftModel]


@dataclass
class ActiveAircraft(AircraftModelBase):
    seats: int
    serial: str
    registrant_name: str
    registrant_county: Optional[str]


@dataclass
class ActiveAircraftList:
    count: int
    loaded_data: List[ActiveAircraft]


@dataclass
class ActiveAircraftCount(AircraftModelBase):
    registrant_county: str
    count: int


@dataclass
class ActiveAircraftCountList:
    count: int
    loaded_data: List[ActiveAircraftCount]


@dataclass
class AircraftCountyAggregate:
    county: str
    agg: int


@dataclass
class AircraftModelAggregate:
    model: str
    agg: List[AircraftCountyAggregate]


@dataclass
class AircraftManufacturerAggregate:
    manufacturer: str
    agg: List[AircraftModelAggregate]


@dataclass
class AircraftAggregate:
    count: int
    loaded_data: List[AircraftManufacturerAggregate]


@dataclass
class ActiveAircraftsPivot:
    count: int
    loaded_data: List[List[Any]]
