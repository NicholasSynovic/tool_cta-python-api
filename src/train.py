from typing import Literal, Optional

from requests import Response

from src import get


class Arrivals:
    def __init__(self, key: str) -> None:
        self.key = key
        self.queryTime: float = -1
        self.endpointTemplate: str = (
            "http://lapi.transitchicago.com/api/1.0/ttarrivals.aspx?outputType=JSON&key="  # noqa: E501
            + self.key
        )

    def get(
        self,
        mapid: int | None = None,
        stpid: int | None = None,
        max: Optional[int] = None,
        rt: Optional[str] = None,
    ) -> Literal[False]:
        if (mapid is None) and (stpid is None):
            return False

        endpoint: str = self.endpointTemplate

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

        print(resp.json())
