from pandas import DataFrame
from requests import Response, get


class SystemAPI:
    """
    Top level interface to working with the CTA System API
    """

    def __init__(self) -> None:
        pass

    def getLStopList(self) -> DataFrame:
        """
        Get data from the L Stop List API endpoint.

        :raises ValueError: Raised if the response status code != 200
        :return: A DataFrame of the response
        :rtype: DataFrame
        """
        resp: Response = get(
            url="https://data.cityofchicago.org/resource/8pix-ypme.json",
            timeout=60,
        )

        if resp.status_code != 200:
            raise ValueError("System API endpoint status code != 200")

        df: DataFrame = DataFrame(data=resp.json())
        df["latitude"] = df["location"].str.get("latitude")
        df["longitude"] = df["location"].str.get("longitude")
        df.drop(columns=["location"], inplace=True)

        return df
