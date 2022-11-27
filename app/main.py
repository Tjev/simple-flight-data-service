import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from fastapi import FastAPI

DATA_DIR = "faa_data/"


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
    registrant_county: str


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


def load_data(data_dir: str) -> Dict[str, pd.DataFrame]:
    df_dict: Dict[str, pd.DataFrame] = dict()
    for file in os.scandir(data_dir):
        if not file.is_file():
            continue

        file_path = Path(file)
        if file_path.suffix != ".parquet":
            continue

        df_dict[file_path.stem] = pd.read_parquet(file_path)

    return df_dict


faa_data = load_data(DATA_DIR)
app = FastAPI()


@app.get("/loaded_data", response_model=DataFrameSummaryList)
def get_loaded_data():
    loaded_data: List[pd.DataFrame] = list()
    for name, df in faa_data.items():
        df_summary = DataFrameSummary(name, df.columns.to_list(), len(df))
        loaded_data.append(df_summary)
    return DataFrameSummaryList(len(loaded_data), loaded_data)


@app.get("/aircraft_models", response_model=AircraftModelList)
def get_aircraft_models(skip: int = 0, limit: int = 100):
    df = faa_data["aircraft_models"][
        ["manufacturer", "model", "seats"]
    ].drop_duplicates()

    all_models = list(map(lambda row: AircraftModel(*row), df.values.tolist()))
    return AircraftModelList(len(all_models), all_models[skip : skip + limit])


@app.get("/active_aircrafts", response_model=ActiveAircraftList)
def get_aircrafts_by_manufacturer_and_model(
    model: Optional[str] = None, manufacturer: Optional[str] = None
):
    air_df = faa_data["aircraft"][
        ["aircraft_serial", "aircraft_model_code", "name", "county", "status_code"]
    ]
    air_df = air_df[air_df.status_code == "A"].drop("status_code", axis=1)

    model_df = faa_data["aircraft_models"][
        ["aircraft_model_code", "manufacturer", "model", "seats"]
    ]

    if model and manufacturer:
        model_df = model_df[
            (model_df.model == model) & (model_df.manufacturer == manufacturer)
        ]
    elif model:
        model_df = model_df[model_df.model == model]
    elif manufacturer:
        model_df = model_df[model_df.manufacturer == manufacturer]

    model_df.set_index("aircraft_model_code", inplace=True)

    merged_df = model_df.merge(air_df, right_on="aircraft_model_code", left_index=True)
    merged_df.drop("aircraft_model_code", axis=1, inplace=True)

    all_active = list(map(lambda row: ActiveAircraft(*row), merged_df.values.tolist()))
    return ActiveAircraftList(len(all_active), all_active)


def recursive_dictify(df: pd.DataFrame):
    if len(df.columns) == 1:
        if df.values.size == 1:
            return df.values[0][0].item()
        return df.values.squeeze()
    next_col = df.columns[0]
    grouped = df.groupby(next_col)
    d = [{next_col: k, "agg": recursive_dictify(g.iloc[:, 1:])} for k, g in grouped]
    return d


@app.get("/agg_active_aircrafts", response_model=AircraftAggregate)
def get_aggregated_active_aircrafts():
    # Returns nested objects (groupby)
    air_df_col = ["aircraft_serial", "aircraft_model_code", "county", "status_code"]
    air_df = faa_data["aircraft"][air_df_col]
    air_df = air_df[air_df.status_code == "A"].drop("status_code", axis=1)

    model_df_col = ["aircraft_model_code", "manufacturer", "model"]
    model_df = faa_data["aircraft_models"][model_df_col]
    model_df.set_index("aircraft_model_code", inplace=True)

    merged_df = model_df.merge(air_df, right_on="aircraft_model_code", left_index=True)
    merged_df.drop("aircraft_model_code", axis=1, inplace=True)

    counts_df = (
        merged_df.groupby(["manufacturer", "model", "county"])
        .size()
        .reset_index(name="count")
    )

    res = recursive_dictify(counts_df)
    return AircraftAggregate(len(res), res)


@app.get("/agg_active_aircrafts2", response_model=ActiveAircraftCountList)
def get_aggregated_active_aircrafts2():
    # Version which returns list of records
    air_df_cols = ["aircraft_serial", "aircraft_model_code", "county", "status_code"]
    air_df = faa_data["aircraft"][air_df_cols]
    air_df = air_df[air_df.status_code == "A"].drop("status_code", axis=1)

    model_df_cols = ["aircraft_model_code", "manufacturer", "model"]
    model_df = faa_data["aircraft_models"][model_df_cols]
    model_df.set_index("aircraft_model_code", inplace=True)

    merged_df = model_df.merge(air_df, right_on="aircraft_model_code", left_index=True)
    merged_df.drop("aircraft_model_code", axis=1, inplace=True)

    counts_df = (
        merged_df.groupby(["manufacturer", "model", "county"])
        .size()
        .reset_index(name="count")
    )

    all_active = list(
        map(lambda row: ActiveAircraftCount(*row), counts_df.values.tolist())
    )
    return ActiveAircraftCountList(len(all_active), all_active)


@app.get("/active_aircrafts_pivot")
def get_active_aircrafts_pivot():
    air_df_cols = ["aircraft_serial", "aircraft_model_code", "county", "status_code"]
    air_df = faa_data["aircraft"][air_df_cols]
    air_df = air_df[air_df.status_code == "A"].drop("status_code", axis=1)

    model_df_cols = ["aircraft_model_code", "manufacturer", "model"]
    model_df = faa_data["aircraft_models"][model_df_cols]
    model_df.set_index("aircraft_model_code", inplace=True)

    merged_df = model_df.merge(air_df, right_on="aircraft_model_code", left_index=True)
    merged_df.drop("aircraft_model_code", axis=1, inplace=True)

    pivot = merged_df.pivot_table(
        "aircraft_serial",
        index=["manufacturer", "model"],
        columns=["county"],
        aggfunc="size",
        fill_value=0,
    )
    pivot = pivot.astype("int32").replace(0, "NULL").reset_index()

    # This return separated columns and rows...
    # return {"columns": pivot.columns.to_list(), "data": pivot.values.tolist()}

    return {"data": [pivot.columns.to_list(), *pivot.values.tolist()]}
