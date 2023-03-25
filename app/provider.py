import os
from pathlib import Path
from typing import Dict, List

import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine


class DataProvider:
    def __init__(self, data_dir: str) -> None:
        # Yes, this is an embedded password... this is purely a PoC application...
        # engine = create_engine('postgresql://postgres:test123@localhost:5432/sfsdb')
        engine = create_engine('postgresql://server@pa200-postgres:test123@pa200-postgres.postgres.database.azure.com:5432/sfsdb')

        # # Use just for populating the db from parquet files present on disk
        # self._init_db(data_dir, engine)

        # Use for regular usage of the app
        self._load_data_from_db(engine)

    def _init_db(self, data_dir: str, engine: Engine):
        self._load_data_from_parquet(data_dir)
        print("parquets loaded to memory...")
        self._write_df_dict_to_db(engine)

    def _load_data_from_parquet(self, data_dir: str) -> None:
        df_dict: Dict[str, pd.DataFrame] = dict()
        for file in os.scandir(data_dir):
            if not file.is_file():
                continue

            file_path = Path(file)
            if file_path.suffix != ".parquet":
                continue

            df_dict[file_path.stem] = pd.read_parquet(file_path)

        self._df_dict = df_dict

    def _write_df_dict_to_db(self, engine: Engine):
        for k, v in self._df_dict.items():
            df = self._df_dict[k]
            df.to_sql(k, engine)
            print(f"executed pd.to_sql({k})")

    def _load_data_from_db(self, engine: Engine):
        df_dict: Dict[str, pd.DataFrame] = dict()
        inspector = inspect(engine)
        for table_name in inspector.get_table_names():
            df_dict[table_name] = pd.read_sql_table(table_name, engine)

        self._df_dict = df_dict

    def get_available_datasets(self) -> List[str]:
        return list(self._df_dict.keys())

    def get_dataset(self, dataset: str) -> pd.DataFrame:
        return self._df_dict[dataset]

