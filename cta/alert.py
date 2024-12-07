from time import time

from pandas import DataFrame
from requests import Response

from cta.api.directors.alert import AlertAPIDirector


class AlertAPI:
    """
    Top level interface to working with the CTA Customer Alerts API
    """

    def __init__(self) -> None:
        """
        Instantiate AlertAPI interface
        """
        self.director: AlertAPIDirector = AlertAPIDirector()

    def route_status(self, **kwargs) -> DataFrame:
        """
        Get data from the Route Status API endpoint.

        Inherits parameters from `cta.api.directors.alert.AlertAPIDirector.getRouteStatus()`

        :raises ValueError: Raised if the response status code != 200
        :return: A DataFrame of the response
        :rtype: DataFrame
        """  # noqa: E501
        resp: Response = self.director.getRouteStatus(**kwargs)

        if resp.status_code != 200:
            raise ValueError("Route Status API endpoint status code != 200")

        ctaRoutes: dict = resp.json()["CTARoutes"]["RouteInfo"]

        df: DataFrame = DataFrame(data=ctaRoutes)
        df["RouteURL"] = df["RouteURL"].str.get("#cdata-section")
        df["Time"] = int(time())
        df.reset_index(drop=True, inplace=True)

        return df

    def detailed_status(self, **kwargs) -> DataFrame:
        """
        Get data from the Detailed Status API endpoint.

        Inherits parameters from `cta.api.directors.alert.AlertAPIDirector.getDetailedAlerts()`

        :raises ValueError: Raised if the response status code != 200
        :return: A DataFrame of the response
        :rtype: DataFrame
        """  # noqa: E501
        resp: Response = self.director.getDetailedAlerts(**kwargs)

        if resp.status_code != 200:
            raise ValueError("Detailed Status API endpoint status code != 200")

        ctaRoutes: dict = resp.json()["CTARoutes"]["RouteInfo"]

        df: DataFrame = DataFrame(data=ctaRoutes)
        df.reset_index(drop=True, inplace=True)

        return df
