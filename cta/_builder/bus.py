import functools
import typing

import cta.api.builders


class BusAPIBuilder:
    """
     _summary_

    _extended_summary_
    """

    def __init__(
        self,
        key: str,
        locale: str = "en",
        format: typing.Literal["xml", "json"] = "json",
    ) -> None:
        """
        __init__ _summary_

        _extended_summary_

        :param key: _description_
        :type key: str
        :param locale: _description_, defaults to "en"
        :type locale: str, optional
        :param format: _description_, defaults to "json"
        :type format: typing.Literal[&quot;xml&quot;, &quot;json&quot;], optional
        :raises ValueError: _description_
        """  # noqa: E501
        if (format != "xml") and (format != "json"):
            raise ValueError("Format must be either `xml` or `json`")

        self.key = key
        self.locale = locale
        self.format = format

        self.constructor: functools.partial = functools.partial(
            cta.api.builders._constructAPI,
            key=self.key,
            locale=self.locale,
            format=self.format,
        )

    def buildTimeAPIURL(self) -> str:
        """
        buildTimeAPIURL _summary_

        _extended_summary_

        :return: _description_
        :rtype: str
        """
        url: str = "http://www.ctabustracker.com/bustime/api/v2/gettime"

        return self.constructor(url=url)

    def buildVehiclesAPIURL(
        self,
        vid: typing.Optional[typing.List[str | int]] = None,
        rt: typing.Optional[typing.List[str | int]] = None,
        tmres: typing.Literal["s", "m"] = "s",
    ) -> str:
        """
        buildVehiclesAPIURL _summary_

        _extended_summary_

        :param vid: _description_, defaults to None
        :type vid: typing.Optional[typing.List[str  |  int]], optional
        :param rt: _description_, defaults to None
        :type rt: typing.Optional[typing.List[str  |  int]], optional
        :param tmres: _description_, defaults to "s"
        :type tmres: typing.Literal[&quot;s&quot;, &quot;m&quot;], optional
        :raises ValueError: _description_
        :raises TypeError: _description_
        :raises TypeError: _description_
        :raises ValueError: _description_
        :raises ValueError: _description_
        :raises ValueError: _description_
        :return: _description_
        :rtype: str
        """
        if vid and rt:
            raise ValueError("`vid` and `rt` cannot be used together")

        if (vid) and (not isinstance(vid, list)):
            raise TypeError("`vid` must be a list")

        if (rt) and (not isinstance(rt, list)):
            raise TypeError("`rt` must be a list")

        if len(vid) > 10:
            raise ValueError("`vid` can only contain 10 elements")

        if len(rt) > 10:
            raise ValueError("`rt` can only contain 10 elements")

        if (tmres != "m") or (tmres != "s"):
            raise ValueError("tmres must be either `s` or `m`")

        url: str = "http://www.ctabustracker.com/bustime/api/v2/getvehicles"

        return self.constructor(
            url=url,
            vid=",".join(vid),
            rt=",".join(rt),
            tmres=tmres,
        )

    def buildRoutesAPIURL(self) -> str:
        """
        buildRoutesAPIURL _summary_

        _extended_summary_

        :return: _description_
        :rtype: str
        """
        url: str = "http://www.ctabustracker.com/bustime/api/v2/getroutes"

        return self.constructor(url=url)

    def buildRouteDirectionsAPIURL(self, rt: str | int) -> str:
        """
        buildRouteDirectionsAPIURL _summary_

        _extended_summary_

        :param rt: _description_
        :type rt: str | int
        :return: _description_
        :rtype: str
        """
        url: str = "http://www.ctabustracker.com/bustime/api/v2/getdirections"

        return self.constructor(url=url, rt=rt)

    def buildStopsAPIURL(self, rt: str | int, dir: str) -> str:
        """
        buildStopsAPIURL _summary_

        _extended_summary_

        :param rt: _description_
        :type rt: str | int
        :param dir: _description_
        :type dir: str
        :return: _description_
        :rtype: str
        """
        url: str = "http://www.ctabustracker.com/bustime/api/v2/getstops"

        return self.constructor(url=url, rt=rt, dir=dir)

    def buildPatternsAPIURL(
        self,
        pid: typing.Optional[typing.List[int]] = None,
        rt: str | int = None,
    ) -> None:
        """
        buildPatternsAPIURL _summary_

        _extended_summary_

        :param pid: _description_, defaults to None
        :type pid: typing.Optional[typing.List[int]], optional
        :param rt: _description_, defaults to None
        :type rt: str | int, optional
        :raises ValueError: _description_
        :raises TypeError: _description_
        :raises ValueError: _description_
        :return: _description_
        :rtype: _type_
        """
        if pid and rt:
            raise ValueError("`pid` and `rt` cannot be used together")

        if (pid) and (not isinstance(pid, list)):
            raise TypeError("`pid` must be a list")

        if len(pid) > 10:
            raise ValueError("`pid` can only contain 10 elements")

        url: str = "http://www.ctabustracker.com/bustime/api/v2/getpatterns"

        return self.constructor(url=url, pid=",".join(pid), rt=rt)

    def buildPredictionsAPIURL(
        self,
        rt: str | int = None,
        stpid: typing.Optional[typing.List[int]] = None,
        vid: typing.Optional[typing.List[int]] = None,
        top: int = None,
    ) -> None:
        """
        buildPredictionsAPIURL _summary_

        _extended_summary_

        :param rt: _description_, defaults to None
        :type rt: str | int, optional
        :param stpid: _description_, defaults to None
        :type stpid: typing.Optional[typing.List[int]], optional
        :param vid: _description_, defaults to None
        :type vid: typing.Optional[typing.List[int]], optional
        :param top: _description_, defaults to None
        :type top: int, optional
        :raises ValueError: _description_
        :raises ValueError: _description_
        :raises TypeError: _description_
        :raises ValueError: _description_
        :raises TypeError: _description_
        :raises ValueError: _description_
        :return: _description_
        :rtype: _type_
        """
        if rt and stpid:
            raise ValueError("`stpid` and `rt` cannot be used together")

        if stpid and vid:
            raise ValueError("`vid` and `stpid` cannot be used together")

        if (stpid) and (not isinstance(stpid, list)):
            raise TypeError("`stpid` must be a list")

        if len(stpid) > 10:
            raise ValueError("`stpid` can only contain 10 elements")

        if (vid) and (not isinstance(vid, list)):
            raise TypeError("`vid` must be a list")

        if len(vid) > 10:
            raise ValueError("`vid` can only contain 10 elements")

        url: str = "http://www.ctabustracker.com/bustime/api/v2/getpredictions"

        return self.constructor(
            url=url,
            rt=rt,
            top=top,
            stpid=",".join(stpid),
            vid=",".join(vid),
        )
