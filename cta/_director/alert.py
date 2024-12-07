from requests import Response, get

from cta.api.builders.alert import AlertAPIBuilder


class AlertAPIDirector:
    """
    Construct and query Customer Alert API endpoints
    """

    def __init__(self) -> None:
        """
        Instantiate an instance of the Alert API builder
        """
        self.builder: AlertAPIBuilder = AlertAPIBuilder()

    def getRouteStatus(self, timeout: int = 60, **kwargs) -> Response:
        """
        Construct and get the response from the Route Status API.

        Inherits the parameters from cta_api.builders.alert.AlertAPIBuilder.buildRouteStatusAPIURL

        :param timeout: Timeout in seconds for when to close the HTTP connection, defaults to 60
        :type timeout: int, optional
        :return: The HTTP GET response
        :rtype: Response
        """  # noqa: E501
        url: str = self.builder.buildRouteStatusAPIURL(**kwargs)

        resp: Response = get(url=url, timeout=timeout)

        return resp

    def getDetailedAlerts(self, timeout: int = 60, **kwargs) -> Response:
        """
        Construct and get the response from the Detailed Alert Status API.

        Inherits the parameters from cta_api.builders.alert.AlertAPIBuilder.buildDetailedAlertsAPIURL

        :param timeout: Timeout in seconds for when to close the HTTP connection, defaults to 60
        :type timeout: int, optional
        :return: The HTTP GET response
        :rtype: Response
        """  # noqa: E501
        url: str = self.builder.buildDetailedAlertsAPIURL(**kwargs)

        resp: Response = get(url=url, timeout=timeout)

        return resp
