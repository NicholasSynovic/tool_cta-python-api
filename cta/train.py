from time import time

from pandas import DataFrame
from requests import Response

from cta.api.directors.train import TrainAPIDirector


class TrainAPI:
    """
    Top level interface to working with the CTA Train Tracker API
    """

    def __init__(self, key: str) -> None:
        """
        Instantiate AlertAPI interface
        """
        self.director: TrainAPIDirector = TrainAPIDirector(key=key)

    def arrivals(self, **kwargs) -> DataFrame:
        """
        Get data from the Arrivals API endpoint.

        Inherits parameters from `cta.api.directors.train.TrainAPIDirector.getArrivals()`

        :raises ValueError: Raised if the response status code != 200
        :return: A DataFrame of the response
        :rtype: DataFrame
        """  # noqa: E501
        resp: Response = self.director.getArrivals(**kwargs)

        if resp.status_code != 200:
            raise ValueError("Arrivals API endpoint status code != 200")

        arrivals: dict = resp.json()["ctatt"]["eta"]

        df: DataFrame = DataFrame(data=arrivals)
        df["Time"] = int(time())
        df.reset_index(drop=True, inplace=True)

        return df

    def follow_this_train(self, **kwargs) -> DataFrame:
        """
        Get data from the Follow This Train API endpoint.

        Inherits parameters from `cta.api.directors.train.TrainAPIDirector.getFollowThisTrain()`

        :raises ValueError: Raised if the response status code != 200
        :return: A DataFrame of the response
        :rtype: DataFrame
        """  # noqa: E501
        resp: Response = self.director.getDetailedAlerts(**kwargs)

        if resp.status_code != 200:
            raise ValueError(
                "Follow This Train API endpoint status code != 200",
            )

        ftt: dict = resp.json()["ctatt"]["eta"]

        df: DataFrame = DataFrame(data=ftt)
        df["Time"] = int(time())
        df.reset_index(drop=True, inplace=True)

        return df

    def locations(self, **kwargs) -> DataFrame:
        """
        Get data from the Locations API endpoint.

        Inherits parameters from `cta.api.directors.train.TrainAPIDirector.getLocations()`

        :raises ValueError: Raised if the response status code != 200
        :return: A DataFrame of the response
        :rtype: DataFrame
        """  # noqa: E501
        resp: Response = self.director.getLocations(**kwargs)

        if resp.status_code != 200:
            raise ValueError("Locations API endpoint status code != 200")

        locations: dict = resp.json()["ctatt"]["route"][0]["train"]

        df: DataFrame = DataFrame(data=locations)
        df["Time"] = int(time())
        df.reset_index(drop=True, inplace=True)

        return df
