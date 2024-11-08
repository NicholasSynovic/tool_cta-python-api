from typing import List, Optional

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

FOLLOWTHISTRAIN_SCHEMA: dict = {
    "$schema": "http://json-schema.org/draft-06/schema#",
    "$ref": "#/definitions/FollowThisTrain",
    "definitions": {
        "FollowThisTrain": {
            "type": "object",
            "additionalProperties": False,
            "properties": {"ctatt": {"$ref": "#/definitions/Ctatt"}},
            "required": ["ctatt"],
            "title": "FollowThisTrain",
        },
        "Ctatt": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "tmst": {"type": "string", "format": "date-time"},
                "errCd": {"type": "string", "format": "integer"},
                "errNm": {"type": "null"},
                "position": {"$ref": "#/definitions/Position"},
                "eta": {
                    "type": "array",
                    "items": {"$ref": "#/definitions/Eta"},
                },
            },
            "required": ["errCd", "errNm", "eta", "position", "tmst"],
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
                "rt": {"$ref": "#/definitions/Rt"},
                "destSt": {"type": "string", "format": "integer"},
                "destNm": {"$ref": "#/definitions/DestNm"},
                "trDr": {"type": "string", "format": "integer"},
                "prdt": {"type": "string", "format": "date-time"},
                "arrT": {"type": "string", "format": "date-time"},
                "isApp": {"type": "string", "format": "integer"},
                "isSch": {"type": "string", "format": "integer"},
                "isDly": {"type": "string", "format": "integer"},
                "isFlt": {"type": "string", "format": "integer"},
                "flags": {"type": "null"},
            },
            "required": [
                "arrT",
                "destNm",
                "destSt",
                "flags",
                "isApp",
                "isDly",
                "isFlt",
                "isSch",
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
        "Position": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "lat": {"type": "string"},
                "lon": {"type": "string"},
                "heading": {"type": "string", "format": "integer"},
            },
            "required": ["heading", "lat", "lon"],
            "title": "Position",
        },
        "DestNm": {
            "type": "string",
            "enum": ["Loop", "54th/Cermak"],
            "title": "DestNm",
        },
        "Rt": {"type": "string", "enum": ["Pink Line"], "title": "Rt"},
    },
}

LOCATIONS_SCHEMA: dict = {
    "$schema": "http://json-schema.org/draft-06/schema#",
    "$ref": "#/definitions/Locations",
    "definitions": {
        "Locations": {
            "type": "object",
            "additionalProperties": False,
            "properties": {"ctatt": {"$ref": "#/definitions/Ctatt"}},
            "required": ["ctatt"],
            "title": "Locations",
        },
        "Ctatt": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "tmst": {"type": "string", "format": "date-time"},
                "errCd": {"type": "string", "format": "integer"},
                "errNm": {"type": "null"},
                "route": {
                    "type": "array",
                    "items": {"$ref": "#/definitions/Route"},
                },
            },
            "required": ["errCd", "errNm", "route", "tmst"],
            "title": "Ctatt",
        },
        "Route": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "@name": {"type": "string"},
                "train": {
                    "type": "array",
                    "items": {"$ref": "#/definitions/Train"},
                },
            },
            "required": ["@name", "train"],
            "title": "Route",
        },
        "Train": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "rn": {"type": "string", "format": "integer"},
                "destSt": {"type": "string", "format": "integer"},
                "destNm": {"$ref": "#/definitions/DestNm"},
                "trDr": {"type": "string", "format": "integer"},
                "nextStaId": {"type": "string", "format": "integer"},
                "nextStpId": {"type": "string", "format": "integer"},
                "nextStaNm": {"type": "string"},
                "prdt": {"type": "string", "format": "date-time"},
                "arrT": {"type": "string", "format": "date-time"},
                "isApp": {"type": "string", "format": "integer"},
                "isDly": {"type": "string", "format": "integer"},
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
                "lat",
                "lon",
                "nextStaId",
                "nextStaNm",
                "nextStpId",
                "prdt",
                "rn",
                "trDr",
            ],
            "title": "Train",
        },
        "DestNm": {
            "type": "string",
            "enum": [
                "Howard",
                "95th/Dan Ryan",
                "O'Hare",
                "Forest Park",
                "UIC-Halsted",
            ],
            "title": "DestNm",
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

        self.queryTime = Timestamp(
            ts_input=data["ctatt"]["tmst"],
            tz="America/Chicago",
        ).timestamp()

        return DataFrame.from_records(data=data["ctatt"]["eta"])


class FollowThisTrain(API, API_PROTOCOL):
    def __init__(self, key: str) -> None:
        self.key: str = key
        self.queryTime: float = -1
        self.endpointBase: str = f"https://lapi.transitchicago.com/api/1.0/ttfollow.aspx?&outputType=JSON&key={self.key}"  # noqa: E501

    def get(self, runnumber: int) -> DataFrame:
        endpoint = self.endpointBase

        endpoint = endpoint + "&runnumber=" + str(runnumber)

        resp: Response = get(url=endpoint)

        data: dict = resp.json()
        if validateData(data=data, schema=FOLLOWTHISTRAIN_SCHEMA) is False:
            return DataFrame()

        self.queryTime = Timestamp(
            ts_input=data["ctatt"]["tmst"],
            tz="America/Chicago",
        ).timestamp()

        return DataFrame.from_records(data=data["ctatt"]["eta"])


class Locations(API, API_PROTOCOL):
    def __init__(self, key: str) -> None:
        self.key: str = key
        self.queryTime: float = -1
        self.endpointBase: str = f"https://lapi.transitchicago.com/api/1.0/ttpositions.aspx?outputType=JSON&key={self.key}"  # noqa: E501

    def get(self, rt: List[str]) -> List[DataFrame]:
        dfs: dict[str, DataFrame] = {}

        endpoint: str = self.endpointBase

        endpoint = endpoint + "&rt=" + ",".join(rt)

        resp: Response = get(url=endpoint)

        data: dict = resp.json()
        if validateData(data=data, schema=LOCATIONS_SCHEMA) is False:
            return {}

        self.queryTime = Timestamp(
            ts_input=data["ctatt"]["tmst"],
            tz="America/Chicago",
        ).timestamp()

        routes: List[dict] = data["ctatt"]["route"]

        route: dict
        for route in routes:
            line: str = route["@name"]
            df: DataFrame = DataFrame.from_records(data=route["train"])
            dfs[line] = df

        return dfs
