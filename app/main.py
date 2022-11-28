from typing import List, Optional

import pandas as pd
from fastapi import Depends, FastAPI

from . import schemas
from .provider import DataProvider

DATA_DIR = "faa_data/"


app = FastAPI()


def get_data_provider():
    provider = DataProvider(DATA_DIR)
    yield provider


@app.get("/loaded_data", response_model=schemas.DataFrameSummaryList)
def get_loaded_data(provider: DataProvider = Depends(get_data_provider)):
    loaded_data: List[schemas.DataFrameSummary] = list()
    for name in provider.get_available_datasets():
        df = provider.get_dataset(name)
        df_summary = schemas.DataFrameSummary(name, df.columns.to_list(), len(df))
        loaded_data.append(df_summary)
    return schemas.DataFrameSummaryList(len(loaded_data), loaded_data)


@app.get("/aircraft_models", response_model=schemas.AircraftModelList)
def get_aircraft_models(skip: int = 0, limit: int = 100, provider: DataProvider = Depends(get_data_provider)):
    df_cols = ["manufacturer", "model", "seats"]
    df = provider.get_dataset("aircraft_models")[df_cols].drop_duplicates()

    all_models = list(map(lambda row: schemas.AircraftModel(*row), df.values.tolist()))
    return schemas.AircraftModelList(len(all_models), all_models[skip : skip + limit])


@app.get("/active_aircrafts", response_model=schemas.ActiveAircraftList)
def get_aircrafts_by_manufacturer_and_model(
    model: Optional[str] = None, manufacturer: Optional[str] = None, provider: DataProvider = Depends(get_data_provider)
):
    air_df_cols = ["aircraft_serial", "aircraft_model_code", "name", "county", "status_code"]
    air_df = provider.get_dataset("aircraft")[air_df_cols]
    air_df = air_df[air_df.status_code == "A"].drop("status_code", axis=1)

    model_df_cols = ["aircraft_model_code", "manufacturer", "model", "seats"]
    model_df = provider.get_dataset("aircraft_models")[model_df_cols]

    if model and manufacturer:
        model_df = model_df[(model_df.model == model) & (model_df.manufacturer == manufacturer)]
    elif model:
        model_df = model_df[model_df.model == model]
    elif manufacturer:
        model_df = model_df[model_df.manufacturer == manufacturer]

    model_df.set_index("aircraft_model_code", inplace=True)

    merged_df = model_df.merge(air_df, right_on="aircraft_model_code", left_index=True)
    merged_df.drop("aircraft_model_code", axis=1, inplace=True)

    all_active = list(map(lambda row: schemas.ActiveAircraft(*row), merged_df.values.tolist()))
    return schemas.ActiveAircraftList(len(all_active), all_active)


def recursive_dictify(df: pd.DataFrame):
    if len(df.columns) == 1:
        if df.values.size == 1:
            return df.values[0][0].item()
        return df.values.squeeze()
    next_col = df.columns[0]
    grouped = df.groupby(next_col)
    d = [{next_col: k, "agg": recursive_dictify(g.iloc[:, 1:])} for k, g in grouped]
    return d


@app.get("/agg_active_aircrafts", response_model=schemas.AircraftAggregate)
def get_aggregated_active_aircrafts(provider: DataProvider = Depends(get_data_provider)):
    # Returns nested objects (groupby)
    air_df_cols = ["aircraft_serial", "aircraft_model_code", "county", "status_code"]
    air_df = provider.get_dataset("aircraft")[air_df_cols]
    air_df = air_df[air_df.status_code == "A"].drop("status_code", axis=1)

    model_df_cols = ["aircraft_model_code", "manufacturer", "model"]
    model_df = provider.get_dataset("aircraft_models")[model_df_cols]
    model_df.set_index("aircraft_model_code", inplace=True)

    merged_df = model_df.merge(air_df, right_on="aircraft_model_code", left_index=True)
    merged_df.drop("aircraft_model_code", axis=1, inplace=True)

    counts_df = merged_df.groupby(["manufacturer", "model", "county"]).size().reset_index(name="count")

    res = recursive_dictify(counts_df)
    return schemas.AircraftAggregate(len(res), res)


@app.get("/agg_active_aircrafts2", response_model=schemas.ActiveAircraftCountList)
def get_aggregated_active_aircrafts2(provider: DataProvider = Depends(get_data_provider)):
    # Version which returns list of records
    air_df_cols = ["aircraft_serial", "aircraft_model_code", "county", "status_code"]
    air_df = provider.get_dataset("aircraft")[air_df_cols]
    air_df = air_df[air_df.status_code == "A"].drop("status_code", axis=1)

    model_df_cols = ["aircraft_model_code", "manufacturer", "model"]
    model_df = provider.get_dataset("aircraft_models")[model_df_cols]
    model_df.set_index("aircraft_model_code", inplace=True)

    merged_df = model_df.merge(air_df, right_on="aircraft_model_code", left_index=True)
    merged_df.drop("aircraft_model_code", axis=1, inplace=True)

    counts_df = merged_df.groupby(["manufacturer", "model", "county"]).size().reset_index(name="count")

    all_active = list(map(lambda row: schemas.ActiveAircraftCount(*row), counts_df.values.tolist()))
    return schemas.ActiveAircraftCountList(len(all_active), all_active)


@app.get("/active_aircrafts_pivot")
def get_active_aircrafts_pivot(provider: DataProvider = Depends(get_data_provider)):
    air_df_cols = ["aircraft_serial", "aircraft_model_code", "county", "status_code"]
    air_df = provider.get_dataset("aircraft")[air_df_cols]
    air_df = air_df[air_df.status_code == "A"].drop("status_code", axis=1)

    model_df_cols = ["aircraft_model_code", "manufacturer", "model"]
    model_df = provider.get_dataset("aircraft_models")[model_df_cols]
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

    data = [pivot.columns.to_list(), *pivot.values.tolist()]
    # return {"count": len(data), "loaded_data": data}
    return schemas.ActiveAircraftsPivot(len(data), data)
