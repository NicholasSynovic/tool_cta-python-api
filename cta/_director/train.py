from requests import Response, get

from cta.api.builders.train import TrainAPIBuilder


class TrainAPIDirector:
    """
    Construct and query Train Tracker API endpoints
    """

    def __init__(self, key: str) -> None:
        """
        Instantiate an instance of the Train Tracker API builder

        :param key: CTA developer API key
        :type key: str
        """
        self.builder: TrainAPIBuilder = TrainAPIBuilder(key=key)

    def getArrivals(self, timeout: int = 60, **kwargs) -> Response:
        """
        Construct and get the response from the Arrivals API.

        Inherits the parameters from cta.builders.train.TrainAPIBuilder.buildArrivalsAPIURL

        :param timeout: Timeout in seconds for when to close the HTTP connection, defaults to 60
        :type timeout: int, optional
        :return: The HTTP GET response
        :rtype: Response
        """  # noqa: E501
        url: str = self.builder.buildRouteStatusAPIURL(**kwargs)

        resp: Response = get(url=url, timeout=timeout)

        return resp

    def getFollowThisTrain(self, timeout: int = 60, **kwargs) -> Response:
        """
        Construct and get the response from the Follow This Train API.

        Inherits the parameters from cta.builders.train.TrainAPIBuilder.buildFollowThisTrainAPI

        :param timeout: Timeout in seconds for when to close the HTTP connection, defaults to 60
        :type timeout: int, optional
        :return: The HTTP GET response
        :rtype: Response
        """  # noqa: E501
        url: str = self.builder.buildFollowThisTrainAPIURL(**kwargs)

        resp: Response = get(url=url, timeout=timeout)

        return resp

    def getLocations(self, timeout: int = 60, **kwargs) -> Response:
        """
        Construct and get the response from the Locations API.

        Inherits the parameters from cta.builders.train.TrainAPIBuilder.buildLocationsAPI

        :param timeout: Timeout in seconds for when to close the HTTP connection, defaults to 60
        :type timeout: int, optional
        :return: The HTTP GET response
        :rtype: Response
        """  # noqa: E501
        url: str = self.builder.buildLocationsAPIURL(**kwargs)

        resp: Response = get(url=url, timeout=timeout)

        return resp
