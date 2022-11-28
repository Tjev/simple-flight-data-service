import os
from pathlib import Path
from typing import Dict, List

import pandas as pd


class DataProvider:
    def __init__(self, data_dir: str) -> None:
        self._load_data(data_dir)

    def _load_data(self, data_dir: str) -> None:
        df_dict: Dict[str, pd.DataFrame] = dict()
        for file in os.scandir(data_dir):
            if not file.is_file():
                continue

            file_path = Path(file)
            if file_path.suffix != ".parquet":
                continue

            df_dict[file_path.stem] = pd.read_parquet(file_path)

        self._df_dict = df_dict

    def get_available_datasets(self) -> List[str]:
        return list(self._df_dict.keys())

    def get_dataset(self, dataset: str) -> pd.DataFrame:
        return self._df_dict[dataset]
