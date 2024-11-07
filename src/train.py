from typing import Optional

from pandas import DataFrame, Timestamp
from requests import Response

from src import API, API_PROTOCOL, get, validateData

ARRIVALS_SCHEMA: dict = {
    "$schema": "http://json-schema.org/draft-06/schema#",
    "$ref": "#/definitions/LStops",
    "definitions": {
        "LStops": {
            "type": "object",
            "additionalProperties": False,
            "properties": {"ctatt": {"$ref": "#/definitions/Ctatt"}},
            "required": ["ctatt"],
            "title": "LStops",
        },
        "Ctatt": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "tmst": {"type": "string", "format": "date-time"},
                "errCd": {"type": "string", "format": "integer"},
                "errNm": {"type": "null"},
                "eta": {
                    "type": "array",
                    "items": {"$ref": "#/definitions/Eta"},
                },
            },
            "required": ["errCd", "errNm", "eta", "tmst"],
            "title": "Ctatt",
        },
        "Eta": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "staId": {"type": "string", "format": "integer"},
                "stpId": {"type": "string", "format": "integer"},
                "staNm": {"type": "string"},
                "stpDe": {"type": "string"},
                "rn": {"type": "string", "format": "integer"},
                "rt": {"type": "string"},
                "destSt": {"type": "string", "format": "integer"},
                "destNm": {"type": "string"},
                "trDr": {"type": "string", "format": "integer"},
                "prdt": {"type": "string", "format": "date-time"},
                "arrT": {"type": "string", "format": "date-time"},
                "isApp": {"type": "string", "format": "integer"},
                "isSch": {"type": "string", "format": "integer"},
                "isDly": {"type": "string", "format": "integer"},
                "isFlt": {"type": "string", "format": "integer"},
                "flags": {"type": "null"},
                "lat": {"type": "string"},
                "lon": {"type": "string"},
                "heading": {"type": "string", "format": "integer"},
            },
            "required": [
                "arrT",
                "destNm",
                "destSt",
                "flags",
                "heading",
                "isApp",
                "isDly",
                "isFlt",
                "isSch",
                "lat",
                "lon",
                "prdt",
                "rn",
                "rt",
                "staId",
                "staNm",
                "stpDe",
                "stpId",
                "trDr",
            ],
            "title": "Eta",
        },
    },
}


class Arrivals(API, API_PROTOCOL):
    def __init__(self, key: str) -> None:
        self.key = key
        self.queryTime: float = -1
        self.endpointBase: str = (
            "http://lapi.transitchicago.com/api/1.0/ttarrivals.aspx?outputType=JSON&key="  # noqa: E501
            + self.key
        )

    def get(
        self,
        mapid: int | None = None,
        stpid: int | None = None,
        max: Optional[int] = None,
        rt: Optional[str] = None,
    ) -> DataFrame:
        if (mapid is None) and (stpid is None):
            return False

        endpoint: str = self.endpointBase

        if (mapid != "") and (mapid is not None):
            endpoint = endpoint + "&mapid=" + mapid

        if (stpid != "") and (stpid is not None):
            endpoint = endpoint + "&stpid=" + stpid

        if max is not None:
            if max > 0:
                endpoint = endpoint + "&max=" + str(max)

        if (rt != "") and (rt is not None):
            endpoint = endpoint + "&rt=" + rt

        resp: Response = get(url=endpoint)

        data: dict = resp.json()
        if validateData(data=data, schema=ARRIVALS_SCHEMA) is False:
            return DataFrame()

        self.queryTime = Timestamp(ts_input=data["ctatt"]["tmst"]).timestamp()

        return DataFrame.from_records(data=data["ctatt"]["eta"])
