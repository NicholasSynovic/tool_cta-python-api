import requests
from pandas import DataFrame
from requests import Response

from cta import API, API_PROTOCOL, validateData

STOPS_SCHEMA: dict = {
    "$schema": "http://json-schema.org/draft-06/schema#",
    "type": "array",
    "items": {"$ref": "#/definitions/LStop"},
    "definitions": {
        "LStop": {
            "type": "object",
            "properties": {
                "stop_id": {"type": "string", "format": "integer"},
                "direction_id": {"type": "string"},
                "stop_name": {"type": "string"},
                "station_name": {"type": "string"},
                "station_descriptive_name": {"type": "string"},
                "map_id": {"type": "string", "format": "integer"},
                "ada": {"type": "boolean"},
                "red": {"type": "boolean"},
                "blue": {"type": "boolean"},
                "g": {"type": "boolean"},
                "brn": {"type": "boolean"},
                "p": {"type": "boolean"},
                "pexp": {"type": "boolean"},
                "y": {"type": "boolean"},
                "pnk": {"type": "boolean"},
                "o": {"type": "boolean"},
                "location": {"$ref": "#/definitions/Location"},
            },
            "required": [
                "ada",
                "blue",
                "brn",
                "direction_id",
                "g",
                "location",
                "map_id",
                "o",
                "p",
                "pexp",
                "pnk",
                "red",
                "station_descriptive_name",
                "station_name",
                "stop_id",
                "stop_name",
                "y",
            ],
            "title": "LStop",
        },
        "Location": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "latitude": {"type": "string"},
                "longitude": {"type": "string"},
                "human_address": {"type": "string"},
            },
            "required": ["human_address", "latitude", "longitude"],
            "title": "Location",
        },
    },
}


class Stops(API, API_PROTOCOL):
    def __init__(self) -> None:
        self.endpointBase: str = (
            "https://data.cityofchicago.org/resource/8pix-ypme.json"  # noqa: E501
        )

    def get(self) -> DataFrame:
        resp: Response = requests.get(url=self.endpointBase, timeout=60)

        data: dict = resp.json()
        if validateData(data=data, schema=STOPS_SCHEMA) is False:
            return DataFrame()

        df: DataFrame = DataFrame.from_records(data=resp.json())
        return df
