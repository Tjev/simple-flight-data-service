from typing import Optional

from fastapi import Depends, FastAPI

from . import schemas
from .processor import DataProcessor
from .provider import DataProvider

DATA_DIR = "faa_data/"


app = FastAPI()


def get_data_processor():
    provider = DataProvider(DATA_DIR)
    processor = DataProcessor(provider)
    yield processor


@app.get("/loaded_data", response_model=schemas.DataFrameSummaryList)
def get_loaded_data(processor: DataProcessor = Depends(get_data_processor)):
    result = processor.get_loaded_data()
    return schemas.DataFrameSummaryList(len(result), result)


@app.get("/aircraft_models", response_model=schemas.AircraftModelList)
def get_aircraft_models(skip: int = 0, limit: int = 100, processor: DataProcessor = Depends(get_data_processor)):
    result = processor.get_aircraft_models()
    return schemas.AircraftModelList(len(result), result[skip : skip + limit])


@app.get("/active_aircrafts", response_model=schemas.ActiveAircraftList)
def get_aircrafts_by_manufacturer_and_model(
    model: Optional[str] = None,
    manufacturer: Optional[str] = None,
    processor: DataProcessor = Depends(get_data_processor),
):
    result = processor.get_aircrafts_by_manufacturer_and_model(manufacturer, model)
    return schemas.ActiveAircraftList(len(result), result)


@app.get("/agg_active_aircrafts", response_model=schemas.AircraftAggregate)
def get_aggregated_active_aircrafts(processor: DataProcessor = Depends(get_data_processor)):
    # Returns nested objects (groupby)
    result = processor.get_aggregated_active_aircrafts()
    return schemas.AircraftAggregate(len(result), result)


@app.get("/agg_active_aircrafts2", response_model=schemas.ActiveAircraftCountList)
def get_aggregated_active_aircrafts2(processor: DataProcessor = Depends(get_data_processor)):
    # Version which returns list of records
    result = processor.get_aggregated_active_aircrafts2()
    return schemas.ActiveAircraftCountList(len(result), result)


@app.get("/active_aircrafts_pivot")
def get_active_aircrafts_pivot(processor: DataProcessor = Depends(get_data_processor)):
    result = processor.get_active_aircrafts_pivot()
    return schemas.ActiveAircraftsPivot(len(result), result)
