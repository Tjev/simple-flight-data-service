from fastapi.testclient import TestClient

from app.main import DataProcessor, DataProvider, app, get_data_processor

MOCK_DATA_DIR = "tests/mock_data"


def get_mocked_data_processor():
    provider = DataProvider(MOCK_DATA_DIR)
    processor = DataProcessor(provider)
    yield processor


class TestAPIEndpoints:
    client = TestClient(app)
    app.dependency_overrides[get_data_processor] = get_mocked_data_processor

    def _assert_status_ok(self, response):
        assert response.status_code == 200

    def test_endpoints(self):
        response = self.client.get("/loaded_data")
        self._assert_status_ok(response)
        expected = {
            "count": 2,
            "loaded_data": [
                {
                    "name": "aircraft_models",
                    "columns": ["aircraft_model_code", "manufacturer", "model", "seats", "unused"],
                    "rows_count": 5,
                },
                {
                    "name": "aircraft",
                    "columns": ["status_code", "county", "aircraft_serial", "name", "aircraft_model_code", "redundant"],
                    "rows_count": 7,
                }
            ]
        }
        assert response.json() == expected

        response = self.client.get("/aircraft_models")
        self._assert_status_ok(response)
        expected = {
            "count": 5,
            "loaded_data": [
                {
                    "manufacturer": "Smiths",
                    "model": "airplane",
                    "seats": 1,
                },
                {
                    "manufacturer": "Johns",
                    "model": "lietadlo",
                    "seats": 2,
                },
                {
                    "manufacturer": "Georges",
                    "model": "letoun",
                    "seats": 3,
                },
                {
                    "manufacturer": "Dicks",
                    "model": "samolot",
                    "seats": 2,
                },
                {
                    "manufacturer": "Mans",
                    "model": "lennuk",
                    "seats": 1,
                }
            ]
        }
        assert response.json() == expected

        response = self.client.get("/active_aircrafts")
        self._assert_status_ok(response)
        expected = {
            "count": 5,
            "loaded_data": [
                {
                    "manufacturer": "Johns",
                    "model": "lietadlo",
                    "seats": 2,
                    "serial": "b",
                    "registrant_name": "Bea",
                    "registrant_county": "001"
                },
                {
                    "manufacturer": "Johns",
                    "model": "lietadlo",
                    "seats": 2,
                    "serial": "d",
                    "registrant_name": "Daniel",
                    "registrant_county": "001"
                },
                {
                    "manufacturer": "Georges",
                    "model": "letoun",
                    "seats": 3,
                    "serial": "c",
                    "registrant_name": "Chuck",
                    "registrant_county": "002"
                },
                {
                    "manufacturer": "Dicks",
                    "model": "samolot",
                    "seats": 2,
                    "serial": "e",
                    "registrant_name": "Eleanor",
                    "registrant_county": "003"
                },
                {
                    "manufacturer": "Mans",
                    "model": "lennuk",
                    "seats": 1,
                    "serial": "g",
                    "registrant_name": "Gregor",
                    "registrant_county": "999"
                }
            ]
        }
        assert response.json() == expected
        
        response = self.client.get("/agg_active_aircrafts")
        self._assert_status_ok(response)
        expected = {
            "count": 4,
            "loaded_data": [
                {
                "manufacturer": "Dicks",
                "agg": [{
                    "model": "samolot",
                    "agg": [{
                        "county": "003",
                        "agg": 1
                        }]
                    }]
                },
                {
                "manufacturer": "Georges",
                "agg": [{
                    "model": "letoun",
                    "agg": [{
                        "county": "002",
                        "agg": 1
                        }]
                    }]
                },
                {
                "manufacturer": "Johns",
                "agg": [{
                    "model": "lietadlo",
                    "agg": [{
                        "county": "001",
                        "agg": 2
                        }]
                    }]
                },
                {
                "manufacturer": "Mans",
                "agg": [{
                    "model": "lennuk",
                    "agg": [{
                        "county": "999",
                        "agg": 1
                        }]
                    }]
                }
            ]
        }
        assert response.json() == expected

        response = self.client.get("/agg_active_aircrafts2")
        self._assert_status_ok(response)
        expected = {
            "count": 4,
            "loaded_data": [
                {
                "manufacturer": "Dicks",
                "model": "samolot",
                "registrant_county": "003",
                "count": 1
                },
                {
                "manufacturer": "Georges",
                "model": "letoun",
                "registrant_county": "002",
                "count": 1
                },
                {
                "manufacturer": "Johns",
                "model": "lietadlo",
                "registrant_county": "001",
                "count": 2
                },
                {
                "manufacturer": "Mans",
                "model": "lennuk",
                "registrant_county": "999",
                "count": 1
                }
            ]
        }
        assert response.json() == expected

        response = self.client.get("/active_aircrafts_pivot")
        self._assert_status_ok(response)
        expected = {
            "count": 5,
            "loaded_data": [
                [ "manufacturer", "model", "001", "002", "003", "999" ],
                [ "Dicks", "samolot", "NULL", "NULL", 1, "NULL" ],
                [ "Georges", "letoun", "NULL", 1, "NULL", "NULL" ],
                [ "Johns", "lietadlo", 2, "NULL", "NULL", "NULL" ],
                [ "Mans", "lennuk", "NULL", "NULL", "NULL", 1 ]
            ]
        }
        assert response.json() == expected
