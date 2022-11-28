from typing import Any, List, Optional

import pandas as pd

from . import schemas
from .provider import DataProvider


class DataProcessor:
    def __init__(self, provider: DataProvider) -> None:
        self._provider = provider

    def get_loaded_data(self) -> List[schemas.DataFrameSummary]:
        loaded_data: List[schemas.DataFrameSummary] = list()
        for name in self._provider.get_available_datasets():
            df = self._provider.get_dataset(name)
            df_summary = schemas.DataFrameSummary(name, df.columns.to_list(), len(df))
            loaded_data.append(df_summary)
        return loaded_data

    def get_aircraft_models(self) -> List[schemas.AircraftModel]:
        df_cols = ["manufacturer", "model", "seats"]
        df = self._provider.get_dataset("aircraft_models")[df_cols].drop_duplicates()
        all_models = list(map(lambda row: schemas.AircraftModel(*row), df.values.tolist()))
        return all_models

    def get_aircrafts_by_manufacturer_and_model(
        self, model: Optional[str] = None, manufacturer: Optional[str] = None
    ) -> List[schemas.ActiveAircraft]:
        air_df_cols = ["aircraft_serial", "aircraft_model_code", "name", "county", "status_code"]
        air_df = self._provider.get_dataset("aircraft")[air_df_cols]
        air_df = air_df[air_df.status_code == "A"].drop("status_code", axis=1)

        model_df_cols = ["aircraft_model_code", "manufacturer", "model", "seats"]
        model_df = self._provider.get_dataset("aircraft_models")[model_df_cols]

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
        return all_active

    def _recursive_jsonify(self, df: pd.DataFrame) -> Any:
        if len(df.columns) == 1:
            if df.values.size == 1:
                return df.values[0][0].item()
            return df.values.squeeze()
        next_col = df.columns[0]
        grouped = df.groupby(next_col)
        d = [{next_col: k, "agg": self._recursive_jsonify(g.iloc[:, 1:])} for k, g in grouped]
        return d

    def get_aggregated_active_aircrafts(self) -> Any:
        # Returns nested objects (groupby)
        air_df_cols = ["aircraft_serial", "aircraft_model_code", "county", "status_code"]
        air_df = self._provider.get_dataset("aircraft")[air_df_cols]
        air_df = air_df[air_df.status_code == "A"].drop("status_code", axis=1)

        model_df_cols = ["aircraft_model_code", "manufacturer", "model"]
        model_df = self._provider.get_dataset("aircraft_models")[model_df_cols]
        model_df.set_index("aircraft_model_code", inplace=True)

        merged_df = model_df.merge(air_df, right_on="aircraft_model_code", left_index=True)
        merged_df.drop("aircraft_model_code", axis=1, inplace=True)

        counts_df = merged_df.groupby(["manufacturer", "model", "county"]).size().reset_index(name="count")

        res_dict = self._recursive_jsonify(counts_df)
        return res_dict

    def get_aggregated_active_aircrafts2(self) -> List[schemas.ActiveAircraftCount]:
        # Version which returns list of records
        air_df_cols = ["aircraft_serial", "aircraft_model_code", "county", "status_code"]
        air_df = self._provider.get_dataset("aircraft")[air_df_cols]
        air_df = air_df[air_df.status_code == "A"].drop("status_code", axis=1)

        model_df_cols = ["aircraft_model_code", "manufacturer", "model"]
        model_df = self._provider.get_dataset("aircraft_models")[model_df_cols]
        model_df.set_index("aircraft_model_code", inplace=True)

        merged_df = model_df.merge(air_df, right_on="aircraft_model_code", left_index=True)
        merged_df.drop("aircraft_model_code", axis=1, inplace=True)

        counts_df = merged_df.groupby(["manufacturer", "model", "county"]).size().reset_index(name="count")

        all_active = list(map(lambda row: schemas.ActiveAircraftCount(*row), counts_df.values.tolist()))
        return all_active

    def get_active_aircrafts_pivot(self) -> List[Any]:
        air_df_cols = ["aircraft_serial", "aircraft_model_code", "county", "status_code"]
        air_df = self._provider.get_dataset("aircraft")[air_df_cols]
        air_df = air_df[air_df.status_code == "A"].drop("status_code", axis=1)

        model_df_cols = ["aircraft_model_code", "manufacturer", "model"]
        model_df = self._provider.get_dataset("aircraft_models")[model_df_cols]
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

        data = [pivot.columns.to_list(), *pivot.values.tolist()]
        return data
