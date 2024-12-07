from functools import partial
from typing import List, Literal, Optional

import cta.api.builders

SAFE_JOIN: partial = partial(cta.api.builders._safeJoin)
VALID_TYPE: List[str] = ["bus", "rail", "station", "systemwide"]


class AlertAPIBuilder:
    """
    Chicago Transit Authority (CTA) Customer Alerts API endpoint builder

    https://www.transitchicago.com/developers/alerts/
    """  # noqa: E501

    def __init__(
        self,
        outputType: Literal["xml", "json"] = "json",
    ) -> None:
        """
        :param outputType: Specifies the format of the response content, defaults to "json"
        :type outputType: Literal[`xml`, `json`], optional
        :raises ValueError: If outputType is not `xml` or `json`, a ValueError is raised
        """  # noqa: E501
        if (outputType != "xml") and (outputType != "json"):
            raise ValueError("outputType must be either `xml` or `json`")

        self.outputType = outputType

        self.constructor: partial = partial(
            cta.api.builders._constructAPI,
            outputType=self.outputType,
        )

    def buildRouteStatusAPIURL(
        self,
        type: Optional[
            List[
                Literal[
                    "bus",
                    "rail",
                    "station",
                    "systemwide",
                ]
            ]
        ] = None,
        routeid: Optional[List[str]] = None,
        stationid: Optional[List[int]] = None,
    ) -> str:
        """
        Build the Route Status API endpoint

        :param type: Service types to query, defaults to None
        :type type: Optional[ List[ Literal[ `bus`, `rail`, `station`, `systemwide`, ] ] ], optional
        :param routeid: Route ids, defaults to None
        :type routeid: Optional[List[str]], optional
        :param stationid: Station ids, defaults to None
        :type stationid: Optional[List[int]], optional
        :raises TypeError: If `type`, `routeid`, or `stationid` is not a list
        :raises ValueError: If `type` contains values that are not `bus`, `rail`, `station`, `systemwide`
        :return: The Route Status API endpoint
        :rtype: str
        """  # noqa: E501
        if (type) and not isinstance(type, list):
            raise TypeError("`type` must be a list")

        if (routeid) and not isinstance(routeid, list):
            raise TypeError("`routeid` must be a list")

        if (stationid) and not isinstance(stationid, list):
            raise TypeError("`stationid` must be a list")

        if type is not None:
            _type: str
            for _type in type:
                try:
                    VALID_TYPE.index(_type)
                except ValueError:
                    raise ValueError(
                        f"`{_type}` is not a valid input to parameter `type`"
                    )  # noqa: E501

        url: str = "http://www.transitchicago.com/api/1.0/routes.aspx"

        return self.constructor(
            url=url,
            type=SAFE_JOIN(data=type),
            routeid=SAFE_JOIN(data=routeid),
            stationid=SAFE_JOIN(data=stationid),
        )

    def buildDetailedAlertsAPIURL(
        self,
        activeonly: bool = None,
        accessibility: bool = None,
        planned: bool = None,
        bystartdate: int = None,
        recentdays: int = None,
        routeid: Optional[List[str]] = None,
        stationid: Optional[List[int]] = None,
    ) -> str:
        """
        Build the Detailed Status API endpoint

        :param activeonly: Only get currently active alerts if True, defaults to None
        :type activeonly: bool, optional
        :param accessibility: Exclude accessibility alerts if False, defaults to None
        :type accessibility: bool, optional
        :param planned: Exclude common planned alerts if False, defaults to None
        :type planned: bool, optional
        :param bystartdate: Yields events with a start date before the one specified (format: yyyyMMdd), defaults to None
        :type bystartdate: int, optional
        :param recentdays: Yields events that have started within *X* number of days before today, defaults to None
        :type recentdays: int, optional
        :param routeid: Route ids, defaults to None
        :type routeid: Optional[List[str]], optional
        :param stationid: Station ids, defaults to None
        :type stationid: Optional[List[int]], optional
        :raises TypeError: If `routeid` or `stationid` is not a list or if `bystartdate` or `recentdays` is not an int
        :return: The Detailed Status API endpoint
        :rtype: str
        """  # noqa: E501
        if (routeid) and not isinstance(routeid, list):
            raise TypeError("`routeid` must be a list")

        if (stationid) and not isinstance(stationid, list):
            raise TypeError("`stationid` must be a list")

        if (bystartdate) and not isinstance(bystartdate, int):
            raise TypeError("`bystartdate` must be a int")

        if (recentdays) and not isinstance(recentdays, int):
            raise TypeError("`recentdays` must be a int")

        url: str = "http://www.transitchicago.com/api/1.0/routes.aspx"

        return self.constructor(
            url=url,
            activeonly=activeonly,
            accessibility=accessibility,
            planned=planned,
            routeid=SAFE_JOIN(data=routeid),
            stationid=SAFE_JOIN(data=stationid),
            bystartdate=bystartdate,
            recentdays=recentdays,
        )
