from functools import partial
from typing import Literal, Optional

import cta.api.builders


class TrainAPIBuilder:
    """
    Chicago Transit Authority (CTA) Customer Train Tracker API endpoint builder

    https://www.transitchicago.com/developers/traintracker/
    """  # noqa: E501

    def __init__(
        self,
        key: str,
        outputType: Literal["xml", "json"] = "json",
    ) -> None:
        """
        :param key: CTA developer API key
        :type key: str
        :param outputType: Specifies the format of the response content, defaults to "json"
        :type outputType: Literal[`xml`, `json`], optional
        :raises ValueError: If outputType is not `xml` or `json`, a ValueError is raised
        """  # noqa: E501
        if (outputType != "xml") and (outputType != "json"):
            raise ValueError("outputType must be either `xml` or `json`")

        self.key = key
        self.outputType = outputType

        self.constructor: partial = partial(
            cta.api.builders._constructAPI,
            key=self.key,
            outputType=self.outputType,
        )

    def buildArrivalsAPIURL(
        self,
        mapid: Optional[int] = None,
        stpid: Optional[int] = None,
        max: Optional[int] = None,
        rt: Optional[str] = None,
    ) -> str:
        """
        Build the Arrivals API endpoint

        :param mapid: Five-digit code to tell the server which station you’d like to receive predictions for, defaults to None
        :type mapid: Optional[int], optional
        :param stpid: Five-digit code to tell the server which specific stop (in this context, specific platform or platform side within a larger station) you’d like to receive predictions for, defaults to None
        :type stpid: Optional[int], optional
        :param max: Maximum number you’d like to receive (if not specified, all available results for the requested stop or station will be returned), defaults to None
        :type max: Optional[int], optional
        :param rt: Specify a single route for which you’d like results, defaults to None
        :type rt: Optional[str], optional
        :raises ValueError: If `mapid` and `stpid` are both None
        :return: The Train Arrivals API endpoint
        :rtype: str
        """  # noqa: E501
        if (mapid is None) and (stpid is None):
            raise ValueError("Either mapid or stpid must be set")

        url: str = "http://lapi.transitchicago.com/api/1.0/ttarrivals.aspx"

        return self.constructor(
            url=url,
            mapid=mapid,
            stpid=stpid,
            max=max,
            rt=rt,
        )

    def buildFollowThisTrainAPIURL(self, runnumber: int) -> str:
        """
        Build the Follow THis Train API endpoint

        :param runnumber: Specify a single run number for a train for which you’d like a series of upcoming arrival estimations.
        :type runnumber: int
        :return: The Follow This Train API endpoint
        :rtype: str
        """  # noqa: E501
        url: str = "http://lapi.transitchicago.com/api/1.0/ttfollow.aspx"
        return self.constructor(url=url, runnumber=runnumber)

    def buildLocationsAPIURL(self, rt: str) -> str:
        """
        Build the Locations API URL endpoint

        :param rt: Specify one or more routes for which you’d like train location information
        :type rt: int
        :return: The Locations API endpoint
        :rtype: str
        """  # noqa: E501
        url: str = "http://lapi.transitchicago.com/api/1.0/ttpositions.aspx"
        return self.constructor(url=url, rt=rt)
